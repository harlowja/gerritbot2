from __future__ import absolute_import
from __future__ import unicode_literals

from datetime import datetime

import collections
import copy
import functools
import getpass
import json
import os
import select
import string
import threading

from errbot import BotPlugin
from errbot import botcmd

import cachetools
import notifier
import paramiko
import retrying

import six
from tabulate import tabulate


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
    try:
        client.connect(hostname, username=username,
                       port=port, key_filename=key_filename)
    except paramiko.SSHException:
        # TODO(harlowja): add something into paramiko so that we can
        # actually tell if its connected or not...
        client.connected = False
        raise
    else:
        client.connected = True
        return client


def filter_by_prior(func):

    @six.wraps(func)
    def wrapper(self, event):
        change_id = None
        if isinstance(event, (PatchSetCreated, CommentAdded)):
            change_id = event.change.id
        if change_id is None:
            func(self, event)
            return
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
        if isinstance(event, PatchSetCreated):
            incoming_emails.append(event.change.owner.email)
            incoming_emails.append(event.patch_set.author.email)
            incoming_emails.append(event.patch_set.uploader.email)
            incoming_emails.append(event.uploader.email)
        if isinstance(event, CommentAdded):
            incoming_emails.append(event.change.owner.email)
            incoming_emails.append(event.patch_set.author.email)
            incoming_emails.append(event.patch_set.uploader.email)
            incoming_emails.append(event.author.email)
        incoming_emails = set(email for email in incoming_emails
                              if email is not None)
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


class Entity(object):
    def __init__(self, username, name, email=None):
        self.username = username
        self.name = name
        self.email = email

    @classmethod
    def from_data(cls, data):
        return cls(data['username'], data['name'], email=data.get('email'))


class PatchSet(object):
    def __init__(self, kind, author,
                 inserts, deletes,
                 uploader, revision, created_on):
        self.kind = kind
        self.author = author
        self.inserts = int(inserts)
        self.deletes = int(deletes)
        self.uploader = uploader
        self.revision = revision
        self.created_on = created_on

    @classmethod
    def from_data(cls, data):
        return cls(data['kind'], Entity.from_data(data['author']),
                   data['sizeInsertions'], data['sizeDeletions'],
                   Entity.from_data(data['uploader']), data['revision'],
                   datetime.fromtimestamp(data['createdOn']))


class Change(object):
    def __init__(self, status, commit_message, number,
                 url, project, owner, subject,
                 branch, id, topic=None):
        self.url = url
        self.id = id
        self.number = int(number)
        self.project = project
        self.status = status
        self.commit_message = commit_message
        self.owner = owner
        self.subject = subject
        self.branch = branch
        self.topic = topic

    @classmethod
    def from_data(cls, data):
        return cls(data['status'], data['commitMessage'],
                   data['number'], data['url'],
                   data['project'], Entity.from_data(data['owner']),
                   data['subject'], data['branch'],
                   data['id'], topic=data.get('topic'))


class PatchSetCreated(object):
    def __init__(self, patch_set, change, uploader, created_on):
        self.patch_set = patch_set
        self.change = change
        self.uploader = uploader
        self.created_on = created_on

    @classmethod
    def from_data(cls, data):
        return cls(PatchSet.from_data(data['patchSet']),
                   Change.from_data(data['change']),
                   Entity.from_data(data['uploader']),
                   datetime.fromtimestamp(data['eventCreatedOn']))


class CommentAdded(object):
    def __init__(self, author, change, patch_set, created_on, comment=None):
        self.comment = comment
        self.patch_set = patch_set
        self.change = change
        self.created_on = created_on
        self.author = author

    @classmethod
    def from_data(cls, data):
        return cls(Entity.from_data(data['author']),
                   Change.from_data(data['change']),
                   PatchSet.from_data(data['patchSet']),
                   datetime.fromtimestamp(data['eventCreatedOn']),
                   comment=data.get('comment'))


class GerritWatcher(object):

    SELECT_WAIT = 0.1
    GERRIT_ACTIVITY = "GERRIT_ACTIVITY"

    def __init__(self, log, make_a_client):
        super(GerritWatcher, self).__init__()
        self.dead = threading.Event()
        self.notifier = notifier.Notifier()
        self.log = log
        self.make_a_client = make_a_client

    def run(self, client=None):
        # This is needed (to be an array) since python is sorta crappy
        # about having variables that u change in local functions...
        if client is None:
            client = self.connect()
        clients = [client]

        def retry_if_io_error(excp):
            try_again = isinstance(excp, (paramiko.ChannelException, IOError))
            if try_again:
                self.log.exception("Failed with exception (retrying)",
                                   exc_info=True)
                self.bot_plugin.warn_admins("Gerrit watching failed"
                                            " due to '%s' (retrying)" % excp)
            else:
                self.log.exception("Failed with exception (not retrying)",
                                   exc_info=True)
                self.bot_plugin.warn_admins("Gerrit watching failed"
                                            " due to '%s' (not"
                                            " retrying)" % excp)
            return try_again

        @retrying.retry(
            wait_exponential_multiplier=1000, wait_exponential_max=10000,
            retry_on_exception=retry_if_io_error)
        def run_forever_until_dead():
            if self.dead.is_set():
                return
            client = clients[0]
            if not client.connected:
                clients[0] = client = self.make_a_client()
            try:
                _stdin, stdout, _stderr = client.exec_command(
                    "gerrit stream-events")
                while not self.dead.is_set():
                    rlist, _wlist, _xlist = select.select(
                        [stdout.channel],
                        [], [], self.SELECT_WAIT)
                    if not rlist:
                        continue
                    event_data = {
                        "event": json.loads(stdout.readline()),
                    }
                    self.notifier.notify(
                        self.GERRIT_ACTIVITY, event_data)
            finally:
                client.close()
                client.connected = False

        try:
            run_forever_until_dead()
        finally:
            clients[0].close()
            clients = []


class GerritBotPlugin(BotPlugin):

    #: Known gerrit event types (to the class that can represent them)...
    GERRIT_EVENTS = {
        'change-abandoned': None,
        'change-merged': None,
        'change-restored': None,
        'comment-added': CommentAdded,
        'draft-published': None,
        'merge-failed': None,
        'patchset-created': PatchSetCreated,
        'patchset-notified': None,
        'project-created': None,
        'ref-replicated': None,
        'ref-replication-done': None,
        'ref-updated': None,
        'reviewer-added': None,
        'topic-changed': None,
    }

    #: Initial stats gathered...
    DEF_STATS = {
        'event_types': collections.defaultdict(int),
        'reviewers': collections.defaultdict(int),
        'commenters': collections.defaultdict(int),
        'projects': collections.defaultdict(int),
    }

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
    }

    def __init__(self, bot):
        super(GerritBotPlugin, self).__init__(bot)
        self.watcher = None
        self.watcher_runner = None
        self.seen_reviews = None
        self.statistics = copy.deepcopy(self.DEF_STATS)

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
        summary = self._bot.process_template('comment', {'event': event})
        for room in self.rooms():
            self.send_card(
                to=room,
                link=event.change.url,
                summary=summary)

    @filter_by_email
    @filter_by_prior
    def process_patchset_created(self, event):
        summary = self._bot.process_template('proposal', {'event': event})
        for room in self.rooms():
            if self.config['include_commit_body']:
                self.send_card(
                    body=event.change.commit_message,
                    to=room,
                    link=event.change.url,
                    summary=summary)
            else:
                self.send_card(
                    to=room,
                    link=event.change.url,
                    summary=summary)

    @botcmd(name="gerrit_stats", historize=False,
            split_args_with=string.split)
    def stats(self, msg, args):
        self.log.debug("Stats called with args: %s", args)
        buf = six.StringIO()
        just_tables = sorted(six.iterkeys(self.statistics))
        if args:
            args_pieces = [p.strip() for p in args.split() if p.strip()]
            if args_pieces:
                just_tables = args_pieces
        for i, tbl_name in enumerate(just_tables):
            if tbl_name in self.statistics:
                tbl = []
                header = [tbl_name.replace("_", " ").title(), "Occurrences"]
                if len(self.statistics[tbl_name]) == 0:
                    tbl.append(['N/A', 'N/A'])
                else:
                    for k in sorted(six.iterkeys(self.statistics[tbl_name])):
                        tbl.append([k, self.statistics[tbl_name][k]])
                buf.write(tabulate(tbl, header, tablefmt="pipe"))
                if i + 1 != len(just_tables):
                    buf.write("\n\n")
                else:
                    buf.write("\n")
        return buf.getvalue()

    def process_event(self, event_type, details):
        try:
            event_type = details['event'].pop('type')
        except KeyError:
            return
        self.statistics['event_types'][event_type] += 1
        event_cls = self.GERRIT_EVENTS.get(event_type)
        if event_cls is None:
            self.log.info("Discarding event '%s', event type not known",
                          event_type)
            return
        event = details['event']
        self.log.debug("Processing event %s using cls %s", event, event_cls)
        event = event_cls.from_data(event)
        event_project = None
        if isinstance(event, (PatchSetCreated, CommentAdded)):
            event_project = event.change.project
        if (event_project is None or
                (self.config['projects']
                 and event_project not in self.config['projects'])):
            if event_project:
                self.log.debug("Discarding event '%s', project '%s' not"
                               " registered to receive events from.",
                               event_type, event_project)
            else:
                self.log.debug("Discarding event '%s', project not"
                               " known.", event_type)
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
        self.seen_reviews = cachetools.TTLCache(
            self.config['max_cache_size'],
            self.config['max_cache_seen_ttl'])
        self.statistics = copy.deepcopy(self.DEF_STATS)
        make_a_client = functools.partial(
            make_and_connect_client,
            self.config['gerrit_hostname'],
            self.config['gerrit_user'],
            port=int(self.config['gerrit_port']),
            key_filename=os.path.expanduser(self.config['gerrit_keyfile']))
        self.watcher = GerritWatcher(self.log, make_a_client)
        self.watcher.notifier.register(
            self.watcher.GERRIT_ACTIVITY, self.process_event)
        self.watcher_runner = threading.Thread(
            target=self.watcher.run,
            kwargs={'client': make_a_client()})
        self.watcher_runner.daemon = True
        self.watcher_runner.start()

    def deactivate(self):
        super(GerritBotPlugin, self).deactivate()
        if self.watcher_runner is not None:
            self.watcher.dead.set()
            self.watcher_runner.join()
            self.watcher = None
            self.watcher_runner = None
