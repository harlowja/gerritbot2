from __future__ import absolute_import
from __future__ import unicode_literals

from datetime import datetime

import contextlib
import copy
import getpass
import json
import os
import select
import threading

from errbot import BotPlugin

import cachetools
import notifier
import paramiko
import retrying


def get_gerrit_user():
    user = os.getenv("GERRIT_USER")
    if user:
        return user
    return getpass.getuser()


def make_and_connect_client(hostname, username,
                            port=29418, key_filename=None):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.WarningPolicy())
    client.connect(hostname, username=username,
                   port=port, key_filename=key_filename)
    return client


class OsGerritWatcher(threading.Thread):
    SELECT_WAIT = 0.1
    GERRIT_ACTIVITY = "GERRIT_ACTIVITY"

    def __init__(self, log, config):
        super(OsGerritWatcher, self).__init__()
        self.dead = threading.Event()
        self.notifier = notifier.Notifier()
        self.log = log
        self.config = config

    def run(self):

        def retry_if_io_error(excp):
            try_again = isinstance(excp, (paramiko.ChannelException, IOError))
            if try_again:
                self.log.exception("Failed with exception (retrying)",
                                   exc_info=True)
            else:
                self.log.exception("Failed with exception (not retrying)",
                                   exc_info=True)
            return try_again

        @retrying.retry(
            wait_exponential_multiplier=1000, wait_exponential_max=10000,
            retry_on_exception=retry_if_io_error)
        def run_forever_until_dead():
            if self.dead.is_set():
                return
            client = make_and_connect_client(
                self.config['gerrit_hostname'],
                self.config['gerrit_user'],
                port=int(self.config['gerrit_port']),
                key_filename=os.path.expanduser(self.config['gerrit_keyfile']))
            with contextlib.closing(client):
                _stdin, stdout, _stderr = client.exec_command(
                    "gerrit stream-events")
                while not self.dead.is_set():
                    rlist, _wlist, _xlist = select.select(
                        [stdout.channel], [], [], self.SELECT_WAIT)
                    if not rlist:
                        continue
                    event_data = {"event": json.loads(stdout.readline())}
                    self.notifier.notify(self.GERRIT_ACTIVITY, event_data)

        run_forever_until_dead()


class OsGerritBotPlugin(BotPlugin):

    #: Default configuration template that should be provided...
    DEF_CONFIG = {
        'gerrit_hostname': 'review.openstack.org',
        'gerrit_port': 29418,
        'gerrit_user': get_gerrit_user(),
        'gerrit_keyfile': '~/.ssh/id_rsa.pub',
        'email_suffixes': ['godaddy.com'],
        'emails': [],
        'max_cache_size': 1000,
        'max_cache_seen_ttl': 60 * 60,
    }

    def __init__(self, bot):
        super(OsGerritBotPlugin, self).__init__(bot)
        self.watcher = None
        self.seen_reviews = None

    def configure(self, configuration):
        if not configuration:
            configuration = {}
        configuration.update(copy.deepcopy(self.DEF_CONFIG))
        super(OsGerritBotPlugin, self).configure(configuration)
        self.log.debug("Bot configuration: %s", self.config)

    def ok_by_email(self, event):
        incoming_emails = []
        for k in ['owner', 'author', 'uploader']:
            if k in event['patchSet']:
                try:
                    incoming_emails.append(event['patchSet'][k]['email'])
                except KeyError:
                    pass
        send_message = False
        if len(self.config['email_suffixes']) == 0:
            send_message = True
        else:
            for ok_suffix in self.config['email_suffixes']:
                if ok_suffix == "*":
                    send_message = True
                else:
                    for in_email in incoming_emails:
                        if in_email.endswith(ok_suffix):
                            send_message = True
        for in_email in incoming_emails:
            if in_email in self.config['emails']:
                send_message = True
        return send_message

    def get_configuration_template(self):
        return copy.deepcopy(self.DEF_CONFIG)

    def process_patchset_created(self, event):
        if not self.ok_by_email(event):
            return
        change_id = event['change']['id']
        if self.seen_reviews and change_id in self.seen_reviews:
            return
        created_on = datetime.fromtimestamp(event['patchSet']['createdOn'])
        self.seen_reviews[change_id] = created_on
        inserts = min(0, event['patchSet'].get('sizeInsertions', 0))
        inserts = "+%s" % inserts
        deletes = max(0, event['patchSet'].get('sizeDeletions', 0))
        if deletes == 0:
            deletes = "-0"
        else:
            deletes = str(deletes)
        tpl_params = {
            'created_on': created_on,
            'inserts': inserts,
            'deletes': deletes,
        }
        for k in ['owner', 'author', 'uploader']:
            if k in event['patchSet']:
                tpl_params[k] = copy.deepcopy(event['patchSet'][k])
        tpl_params['change'] = copy.deepcopy(event['change'])
        tpl_params['change']['commitMessageLines'] = []
        for line in tpl_params['change']['commitMessage'].splitlines():
            tpl_params['change']['commitMessageLines'].append(line)
        summary = self._bot.process_template('proposal', tpl_params)
        for room in self.rooms():
            self.send_card(
                body=tpl_params['change']['commitMessage'],
                to=room,
                link=tpl_params['change']['url'],
                summary=summary)

    def process_event(self, event_type, details):
        event_type = None
        try:
            event_type = details['event']['type']
        except (KeyError, TypeError):
            pass
        if not event_type:
            return
        event = details['event']
        self.log.debug(
            "Processing event %s with details: %s", event_type, event)
        event_type_func = "process_%s" % event_type.replace("-", "_")
        try:
            event_func = getattr(self, event_type_func)
        except AttributeError:
            pass
        else:
            try:
                event_func(event)
            except Exception:
                self.log.exception("Failed dispatching event '%s' to '%s'",
                                   event_type, event_type_func)

    def activate(self):
        super(OsGerritBotPlugin, self).activate()
        if not self.config:
            return
        self.seen_reviews = cachetools.TTLCache(
            self.config['max_cache_size'],
            self.config['max_cache_seen_ttl'])
        self.watcher = OsGerritWatcher(self.log, self.config)
        self.watcher.notifier.register(
            self.watcher.GERRIT_ACTIVITY, self.process_event)
        self.watcher.daemon = True
        self.watcher.start()

    def deactivate(self):
        super(OsGerritBotPlugin, self).deactivate()
        if self.watcher is not None:
            self.watcher.dead.set()
            self.watcher.join()
            self.watcher = None
