"""
Configure the application.
"""
from rhobot.application import Application
from timeline_bot.components import load_components

application = Application()

application.pre_init(load_components)

@application.post_init
def post_initialization(bot):
    bot.register_plugin('maintainer')
