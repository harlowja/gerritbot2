# -*- coding: utf-8 -*-

#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from __future__ import absolute_import
from __future__ import unicode_literals

from datetime import datetime

import collections
import copy
import json
import logging
import re
import threading
import time

from errbot import BotPlugin
from errbot import botcmd

import cachetools
import paho.mqtt.client as mqtt
import requests
import tenacity

import six
from six.moves import queue as compat_queue
from tabulate import tabulate

TOMBSTONE = object()


def str_split(text):
    return text.split()


def filter_by_prior(func):
    """Filter that drops/ignores repeated event."""

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
    """Filter that drops/ignores events **not** from certain emails."""

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
    """A object representation of a gerrit author/person/some entity."""

    def __init__(self, username, name, email=None):
        self.username = username
        self.name = name
        self.email = email

    @classmethod
    def from_data(cls, data):
        return cls(data['username'], data['name'], email=data.get('email'))


class PatchSet(object):
    """A object representation of a gerrit patchset."""

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
    """A object representation of a gerrit change."""

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
    """A object representation of a gerrit patchset creation event."""

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
    """A object representation of a gerrit patchset comment addition event."""

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
        'uploaders': collections.defaultdict(int),
        'projects': collections.defaultdict(int),
    }

    #: Default configuration template that should be provided...
    DEF_CONFIG = {
        'firehose_host': 'firehose.openstack.org',
        'firehose_transport': "tcp",
        'firehose_port': 1883,
        'email_suffixes': [],
        'emails': [],
        'include_commit_body': False,
        'max_cache_size': 1000,
        'max_cache_seen_ttl': 60 * 60,
        'projects': [],
        # See: https://pypi.python.org/pypi/tabulate
        'tabulate_format': 'plain',
    }

    def __init__(self, bot):
        super(GerritBotPlugin, self).__init__(bot)
        self.client = None
        self.work_queue = None
        self.seen_reviews = None
        self.processor = None
        self.dying = False
        self.statistics = copy.deepcopy(self.DEF_STATS)

    def configure(self, configuration):
        if not configuration:
            configuration = {}
        configuration.update(copy.deepcopy(self.DEF_CONFIG))
        super(GerritBotPlugin, self).configure(configuration)
        self.log.debug("Bot configuration: %s", self.config)

    def get_configuration_template(self):
        return copy.deepcopy(self.DEF_CONFIG)

    def callback_message(self, message):
        body = message.body
        if body:
            review_ids = re.findall(
               r"http([s])?://review.openstack.org/#/c/(\d+)", body)
            for is_https, review_id in review_ids:
                if is_https:
                    link = "https://"
                else:
                    link = "http://"
                review_url = link + "review.openstack.org/#/c/%s" % review_id
                json_url = link + "review.openstack.org/changes/%s" % review_id
                try:
                    rsp = requests.get(json_url, timeout=10)
                    rsp.raise_for_status()
                except (requests.ConnectionError,
                        requests.ConnectTimeout,
                        requests.RequestException):
                    pass
                else:
                    try:
                        content = rsp.text.split("\n", 1)[1]
                        content = json.loads(content)
                        content['url'] = review_url
                    except (ValueError, TypeError):
                        pass
                    else:
                        summary = self._bot.process_template(
                            'review', {'review': content})
                        self.send_card(
                            to=message.frm,
                            link=review_url,
                            summary=summary)

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
            split_args_with=str_split)
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
                buf.write(tabulate(tbl, header,
                                   tablefmt=self.config['tabulate_format']))
                if i + 1 != len(just_tables):
                    buf.write("\n")
                else:
                    buf.write("\n")
        return buf.getvalue()

    def loop_process_events(self):
        while not self.dying:
            details = self.work_queue.get()
            if details is TOMBSTONE:
                self.work_queue.task_done()
                break
            else:
                try:
                    if not self.dying:
                        self.process_event(details)
                finally:
                    self.work_queue.task_done()

    def process_event(self, details):
        try:
            event_type = details['event'].pop('type')
        except KeyError:
            return
        self.statistics['event_types'][event_type] += 1
        event_cls = self.GERRIT_EVENTS.get(event_type)
        event = details['event']
        if not event_cls:
            self.log.info("Discarding event '%s' with data %s since that"
                          " event type is not known.",
                          event_type, event)
            return
        self.log.debug("Processing event %s using cls %s", event, event_cls)
        event = event_cls.from_data(event)
        event_project = None
        if isinstance(event, (PatchSetCreated, CommentAdded)):
            event_project = event.change.project
        if event_project is not None:
            self.statistics['projects'][event_project] += 1
        if isinstance(event, CommentAdded):
            who = event.author.email
            if who:
                self.statistics['reviewers'][who] += 1
        if isinstance(event, PatchSetCreated):
            who = event.uploader.email
            if who:
                self.statistics['uploaders'][who] += 1
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

    def loop_client_recv(self):
        # The built-in client loop_forever seems busted (it doesn't retry
        # under all exceptions, so just do it ourselves...); arg...

        def on_connect(client, userdata, flags, rc):
            if rc == mqtt.MQTT_ERR_SUCCESS:
                self.log.info("MQTT connected to %s:%s over %s",
                              self.config['firehose_host'],
                              self.config['firehose_port'],
                              self.config['firehose_transport'])
                client.subscribe('#')
            else:
                self.log.error(
                    "MQTT not connected to %s:%s over %s, rc=%s",
                    self.config['firehose_host'],
                    self.config['firehose_port'],
                    self.config['firehose_transport'], rc)

        def on_message(client, userdata, msg):
            if not msg.topic or not msg.payload:
                return
            self.log.info(("Dispatching message on topic=%s"
                           " with payload=%s"), msg.topic, msg.payload)
            try:
                payload = msg.payload
                if isinstance(payload, six.binary_type):
                    payload = payload.decode("utf8")
                details = {'event': json.loads(payload)}
            except (UnicodeError, ValueError):
                self.log.exception(
                    "Received corrupted/invalid payload: %s", msg.payload)
            else:
                self.work_queue.put(details)

        @tenacity.retry(
            wait=tenacity.wait_exponential(multiplier=1, max=30),
            before=tenacity.before_log(self.log, logging.INFO))
        def loop_forever_until_dead():
            if self.dying:
                return
            client = mqtt.Client(transport=self.config['firehose_transport'])
            client.on_connect = on_connect
            client.on_message = on_message
            try:
                client.connect(self.config['firehose_host'],
                               port=int(self.config['firehose_port']))
                max_timeout = 1
                while not self.dying:
                    rc = mqtt.MQTT_ERR_SUCCESS
                    start = time.time()
                    elapsed = 0
                    while rc == mqtt.MQTT_ERR_SUCCESS and (elapsed < max_timeout):
                        rc = client.loop(timeout=max(0, max_timeout - elapsed))
                        elapsed = time.time() - start
                    if not self.dying:
                        time.sleep(0.1)
            except Exception:
                self.log.exception("Failed mqtt client usage, retrying")
                raise

        loop_forever_until_dead()

    def activate(self):
        super(GerritBotPlugin, self).activate()
        self.work_queue = compat_queue.Queue()
        self.dying = False
        self.seen_reviews = cachetools.TTLCache(
            self.config['max_cache_size'],
            self.config['max_cache_seen_ttl'])
        self.statistics = copy.deepcopy(self.DEF_STATS)
        self.client = threading.Thread(target=self.loop_client_recv)
        self.client.daemon = True
        self.client.start()
        self.processor = threading.Thread(target=self.loop_process_events)
        self.processor.daemon = True
        self.processor.start()

    def deactivate(self):
        super(GerritBotPlugin, self).deactivate()
        self.dying = True
        if self.client is not None:
            self.client.join()
            self.client = None
        if self.processor is not None:
            self.work_queue.put(TOMBSTONE)
            self.work_queue.join()
            self.processor.join()
            self.processor = None
