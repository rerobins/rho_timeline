from timeline_bot.components.maintainer import maintainer

from sleekxmpp.plugins.base import register_plugin

def load_components():
    """
    Load the components from this application.
    """
    register_plugin(maintainer)
