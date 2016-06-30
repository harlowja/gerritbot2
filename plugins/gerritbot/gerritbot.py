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
import weakref

from errbot import BotPlugin

import cachetools
import notifier
import paramiko
import retrying

import six


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


def filter_by_prior(func):

    @six.wraps(func)
    def wrapper(self, event):
        change_id = event['change']['id']
        if self.seen_reviews and change_id in self.seen_reviews:
            return
        else:
            func(self, event)
            self.seen_reviews[change_id] = True

    return wrapper


def filter_by_email(func):

    @six.wraps(func)
    def wrapper(self, event):
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
        if send_message:
            func(self, event)

    return wrapper


class GerritWatcher(threading.Thread):
    SELECT_WAIT = 0.1
    GERRIT_ACTIVITY = "GERRIT_ACTIVITY"

    def __init__(self, bot_plugin):
        super(GerritWatcher, self).__init__()
        self.bot_plugin = weakref.proxy(bot_plugin)
        self.dead = threading.Event()
        self.notifier = notifier.Notifier()
        self.log = bot_plugin.log
        self.config = bot_plugin.config

    def run(self):

        def retry_if_io_error(excp):
            try_again = isinstance(excp, (paramiko.ChannelException, IOError))
            if try_again:
                self.log.exception("Failed with exception (retrying)",
                                   exc_info=True)
                self.bot_plugin.warn_admins("Gerrit watching failed"
                                            " due to `%s` (retrying)" % excp)
            else:
                self.log.exception("Failed with exception (not retrying)",
                                   exc_info=True)
                self.bot_plugin.warn_admins("Gerrit watching failed"
                                            " due to `%s` (not"
                                            " retrying)" % excp)
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
            self.log.debug("Connected to gerrit via %s@%s",
                           self.config['gerrit_user'],
                           self.config['gerrit_hostname'])
            with contextlib.closing(client):
                _stdin, stdout, _stderr = client.exec_command(
                    "gerrit stream-events")
                while not self.dead.is_set():
                    rlist, _wlist, _xlist = select.select(
                        [stdout.channel, stderr.channel],
                        [], [], self.SELECT_WAIT)
                    if not rlist:
                        continue
                    for c in rlist:
                        if c is stderr.channel:
                            error = stderr.readline()
                            if not error:
                                raise IOError("Remote server"
                                              " connection closed")
                            raise IOError(error)
                        else:
                            event_data = {
                                "event": json.loads(stdout.readline()),
                            }
                            self.notifier.notify(
                                self.GERRIT_ACTIVITY, event_data)

        run_forever_until_dead()


class GerritBotPlugin(BotPlugin):

    #: Known gerrit event types...
    GERRIT_EVENTS = frozenset([
        'change-abandoned',
        'change-merged',
        'change-restored',
        'comment-added',
        'draft-published',
        'merge-failed',
        'patchset-created',
        'patchset-notified',
        'project-created',
        'ref-replicated',
        'ref-replication-done',
        'ref-updated',
        'reviewer-added',
        'topic-changed',
    ])

    #: Default configuration template that should be provided...
    DEF_CONFIG = {
        'gerrit_hostname': 'review.openstack.org',
        'gerrit_port': 29418,
        'gerrit_user': get_gerrit_user(),
        'gerrit_keyfile': '~/.ssh/id_rsa.pub',
        'email_suffixes': [],
        'emails': [],
        'include_commit_body': False,
        'max_cache_size': 1000,
        'max_cache_seen_ttl': 60 * 60,
        'projects': [],
        'exclude_events': [
            'change-abandoned',
            'change-merged',
            'change-restored',
            'comment-added',
            'draft-published',
            'merge-failed',
            'patchset-notified',
            'project-created',
            'ref-replicated',
            'ref-replication-done',
            'ref-updated',
            'reviewer-added',
            'topic-changed',
        ],
    }

    def __init__(self, bot):
        super(GerritBotPlugin, self).__init__(bot)
        self.watcher = None
        self.seen_reviews = None

    def configure(self, configuration):
        if not configuration:
            configuration = {}
        configuration.update(copy.deepcopy(self.DEF_CONFIG))
        super(GerritBotPlugin, self).configure(configuration)
        self.log.debug("Bot configuration: %s", self.config)

    def get_configuration_template(self):
        return copy.deepcopy(self.DEF_CONFIG)

    @filter_by_email
    @filter_by_prior
    def process_comment_added(self, event):
        tpl_params = {}
        for k in ['author', 'change', 'comment']:
            tpl_params[k] = copy.deepcopy(event[k])
        summary = self._bot.process_template('comment', tpl_params)
        for room in self.rooms():
            self.send_card(
                to=room,
                link=tpl_params['change']['url'],
                summary=summary)

    @filter_by_email
    @filter_by_prior
    def process_patchset_created(self, event):
        created_on = datetime.fromtimestamp(event['patchSet']['createdOn'])
        inserts = event['patchSet'].get('sizeInsertions', 0)
        inserts = "+%s" % inserts
        deletes = event['patchSet'].get('sizeDeletions', 0)
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
            if self.config['include_commit_body']:
                self.send_card(
                    body=tpl_params['change']['commitMessage'],
                    to=room,
                    link=tpl_params['change']['url'],
                    summary=summary)
            else:
                self.send_card(
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
            "Processing event '%s' with details: %s", event_type, event)
        if event_type not in self.GERRIT_EVENTS:
            self.log.info("Discarding event '%s', event type not known",
                          event_type)
            return
        if event_type in self.config['exclude_events']:
            self.log.debug("Discarding event '%s', event type marked"
                           " to be excluded from processing.",
                           event_type)
            return
        event_project = event.get('project')
        if (self.config['projects']
                and event_project not in self.config['projects']):
            self.log.debug("Discarding event '%s', project '%s' not"
                           " registered to receive events from.",
                           event_type, event_project)
            return
        event_type_func = "process_%s" % event_type.replace("-", "_")
        try:
            event_func = getattr(self, event_type_func)
        except AttributeError:
            self.log.debug("Discarding event '%s', no handler found.",
                           event_type)
        else:
            try:
                event_func(event)
            except Exception:
                self.log.exception("Failed dispatching event '%s'"
                                   " to '%s'", event_type,
                                   event_type_func)

    def activate(self):
        super(GerritBotPlugin, self).activate()
        if not self.config:
            return
        self.seen_reviews = cachetools.TTLCache(
            self.config['max_cache_size'],
            self.config['max_cache_seen_ttl'])
        self.watcher = GerritWatcher(self)
        self.watcher.notifier.register(
            self.watcher.GERRIT_ACTIVITY, self.process_event)
        self.watcher.daemon = True
        self.watcher.start()

    def deactivate(self):
        super(GerritBotPlugin, self).deactivate()
        if self.watcher is not None:
            self.watcher.dead.set()
            self.watcher.join()
            self.watcher = None
