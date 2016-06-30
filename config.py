import logging
import os

BOT_DATA_DIR = os.path.join(os.getcwd(), 'data')
BOT_EXTRA_PLUGIN_DIR = os.path.join(os.getcwd(), 'plugins')
STORAGE = 'Shelf'

BOT_LOG_FILE = None
BOT_LOG_LEVEL = logging.DEBUG

# The admins that can send the bot special commands...
BOT_ADMINS = []

# The following will change depending on the backend selected...
BACKEND = 'IRC' 
BOT_IDENTITY = {
    'server': 'chat.freenode.net',
    'nickname': 'gerritbot2',
}
COMPACT_OUTPUT = False
CORE_PLUGINS = ('ACLs', 'Help', 'Health', 'Plugins', 'ChatRoom')

# Rooms we will join by default.
CHATROOM_PRESENCE = [
    '#bot_testing',
]
