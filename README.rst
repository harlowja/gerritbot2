=====================
Gerritbot version 2.0
=====================

A (errbot based) gerrit bot/plugin that can communicate with the
gerrit event MQTT stream and receive events and drop those events into
slack (or other backends) using built-in templates.

Ensure that before using you:

* Edit ``config.py`` and put in your needed desired backend (ie for slack
  this will require slack keys and username and
  the room the bot will join).

The plugin also has the following configuration (the following is the
default)::


    {
        # MQTT firehose settings/host/port.
        'firehose_host': 'firehose.openstack.org',
        'firehose_transport': "tcp",
        'firehose_port': 1883,

        # patchset reviews with only these suffixes (or equivalent
        # emails will be allowed); if suffixes is empty than all emails
        # will be valid
        'email_suffixes': [],
        'emails': [],

        # max number of previous reviews to remember
        'max_cache_size': 1000,

        # max time previous reviews stay in cache (messages for reviews
        # that are repeated in this timeframe will be dropped)
        'max_cache_seen_ttl': 60 * 60,

        # which projects to accept events from (if empty then this
        # implies all projects are valid).
        'projects': [],
    }

It can be overriden by interacting with the errbot using the
normal errbot configuration mechanism listed at:

  http://errbot.io/en/latest/user_guide/administration.html#configuration
