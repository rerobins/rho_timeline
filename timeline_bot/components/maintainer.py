"""
Will search through the database for all of the interval nodes that have been added to the database that are not apart
of a relative time line or universal timeline.
"""
from sleekxmpp.plugins.base import base_plugin
from rhobot.components.storage import StoragePayload
from rhobot.components.storage.namespace import NEO4J
from rhobot.components.storage.events import STORAGE_FOUND
from rhobot.namespace import TIMELINE

class Maintainer(base_plugin):
    name = 'timeline_maintainer'
    description = 'Search database for unassociated intervals and associate them with the correct timeline.'
    depenedencies = {'rho_bot_scheduler', 'rho_bot_storage_client'}

    # Query that is used to find unassociated intervals.
    query = """
    MATCH (n:`%s`)
    WHERE NOT (n)-[:`%s`]->(:`%s`)
    RETURN n
    LIMIT 1
    """ % (TIMELINE.Interval, TIMELINE.timeline, TIMELINE.RelativeTimeLine)

    # Delays
    work_to_do_delay = 1.0
    no_work_delay = 600.0

    def plugin_init(self):
        """
        Subscribe to joining the channel event to see if there are missing associations.
        :return:
        """
        self.xmpp.add_event_handler(STORAGE_FOUND, self._storage_found)
        self.query = ' '.join(self.query.split())

    def post_init(self):
        """
        Finish configuring this plugin.
        :return:
        """
        super(Maintainer, self).post_init()
        self._storage_client = self.xmpp['rho_bot_storage_client']
        self._scheduler = self.xmpp['rho_bot_scheduler']

    def _storage_found(self, event):
        """
        Can now kick off the commands to be able to start finding the intervals.
        :param event:
        :return:
        """
        self.xmpp.del_event_handler(event, self._storage_found)
        self._scheduler.schedule_task(self._start_process, delay=self.work_to_do_delay)

    def _start_process(self):
        """
        Kick off the processes to find the intervals and add them to the chain.
        :return:
        """
        promise = self._scheduler.defer(lambda: dict())

        promise.then(self._scheduler.generate_promise_handler(self._reschedule, self._work_to_do_delay),
                     self._scheduler.generate_promise_handler(self._reschedule, self._no_work_delay))

    def _find_work_node(self, session):
        """
        Find a node to work over.
        :param session:
        :return:
        """
        payload = StoragePayload()
        payload.add_property(key=NEO4J.cypher, value=self.query)
        promise = self._storage_client.execute(payload).then(
            self._scheduler.generate_promise_handler(self._handle_cypher_result, session))

        return promise

    def _handle_cypher_result(self, result, session):
        """
        Handle the results from the cypher result.  If there as no result found then reject the promise, otherwise
        update the session with the uri of the node, and
        :param result:
        :param session:
        :return:
        """
        if not result.results:
            raise Exception('No results to work')

        node_uri = result.results[0].about

        session['node'] = node_uri

        return session

    def _reschedule(self, session, delay):
        """
        Reschedule the task.
        :param session:
        :param delay:
        :return:
        """
        self._scheduler.schedule_task(self._start_process, delay=delay)

maintainer = Maintainer
