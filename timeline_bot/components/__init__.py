from sleekxmpp.plugins.base import register_plugin
from timeline_bot.components.maintainer import timeline_maintainer
from timeline_bot.components.create_listener import CreateListener


def load_components():
    """
    Load the components from this application.
    """
    register_plugin(timeline_maintainer)
    register_plugin(CreateListener)

