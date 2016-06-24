import logging

BACKEND = 'Slack' 
BOT_DATA_DIR = './data'
BOT_EXTRA_PLUGIN_DIR = './plugins'

BOT_LOG_FILE = None
BOT_LOG_LEVEL = logging.DEBUG

BOT_ADMINS = ['@jxharlow']
BOT_IDENTITY = {
    'token': '<a-key>',
}

CHATROOM_PRESENCE = [
    '#cloud',
]

CHATROOM_FN = 'osslackgerrit'

CORE_PLUGINS = ('ACLs', 'Help', 'Health', 'Plugins')

