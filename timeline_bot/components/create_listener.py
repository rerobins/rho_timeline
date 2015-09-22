"""
Listener for interval create objects.
"""
from sleekxmpp.plugins.base import base_plugin
from rhobot.namespace import TIMELINE
from rhobot.components.storage import StoragePayload

class CreateListener(base_plugin):

    name = 'create_listener'
    description = 'Create Listener'
    dependencies = {'rho_bot_rdf_publish', 'timeline_maintainer', }

    interval_types = {str(TIMELINE.Interval), }

    def plugin_init(self):
        pass

    def post_init(self):
        super(CreateListener, self).post_init()
        self._rdf_publish = self.xmpp['rho_bot_rdf_publish']
        self._timeline_maintainer = self.xmpp['timeline_maintainer']

        self._rdf_publish.add_create_handler(self._timeline_published)
        self._rdf_publish.add_update_handler(self._timeline_published)

    def _timeline_published(self, rdf_payload):
        """
        A create was published so the node should be added to a timeline.
        :param rdf_payload:
        :return:
        """
        # TODO: I don't understand why this has to be |en
        storage = StoragePayload(rdf_payload['form|en'])

        types = set(storage.types)
        intersection = types.union(self.interval_types)

        if len(intersection) == len(self.interval_types):
            self._timeline_maintainer.handle_interval_modification(storage.about)
