"""
Will search through the database for all of the interval nodes that have been added to the database that are not apart
of a relative time line or universal timeline.
"""
import isodate
import datetime
import logging
from sleekxmpp.plugins.base import base_plugin
from rhobot.components.storage import StoragePayload
from rhobot.components.storage.enums import UpdateFlags
from rhobot.components.storage.namespace import NEO4J
from rhobot.components.storage.events import STORAGE_FOUND
from rhobot.namespace import TIMELINE
from rdflib.namespace import DCTERMS

logger = logging.getLogger(__name__)


class Maintainer(base_plugin):
    name = 'timeline_maintainer'
    description = 'Search database for unassociated intervals and associate them with the correct timeline.'
    dependencies = {'rho_bot_scheduler', 'rho_bot_storage_client', 'rho_bot_get_or_create',
                    'rho_bot_representation_manager', 'rho_bot_rdf_publish'}

    # Query that is used to find unassociated intervals.
    query = """
    MATCH (node:`%s`)
    WHERE NOT (node)-[:`%s`]->(:`%s`)
    RETURN node
    LIMIT 1
    """ % (TIMELINE.Interval, TIMELINE.timeline, TIMELINE.RelativeTimeLine)

    # Delays
    _work_to_do_delay = 1.0
    _no_work_delay = 600.0

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
        self._get_or_create = self.xmpp['rho_bot_get_or_create']
        self._representation_manager = self.xmpp['rho_bot_representation_manager']
        self._rdf_publish = self.xmpp['rho_bot_rdf_publish']

    def _storage_found(self, *args):
        """
        Can now kick off the commands to be able to start finding the intervals.
        :return:
        """
        self.xmpp.del_event_handler(STORAGE_FOUND, self._storage_found)
        self._scheduler.schedule_task(self._start_process, delay=self._work_to_do_delay)

    def _start_process(self):
        """
        Kick off the processes to find the intervals and add them to the chain.
        :return:
        """
        promise = self._scheduler.defer(lambda: dict())
        promise = promise.then(self._find_work_node)
        promise = promise.then(self._get_interval_properties)
        promise = promise.then(self._get_timelines)
        promise = promise.then(self._update_interval)

        promise.then(self._scheduler.generate_promise_handler(self._reschedule, self._work_to_do_delay),
                     self._scheduler.generate_promise_handler(self._reschedule, self._no_work_delay))

    def handle_interval_modification(self, node_uri):
        """
        Handle work done to add/update the interval to the
        :param node_uri:
        :return:
        """
        try:
            promise = self._scheduler.defer(lambda: dict(node_uri=node_uri))
            promise = promise.then(self._get_interval_properties)
            promise = promise.then(self._get_timelines)
            promise = promise.then(self._update_interval)

            return promise
        except Exception as e:
            logger.error('Exception: ', e)

    def _find_work_node(self, session):
        """
        Find a node to work over.
        :param session:
        :return:
        """
        def _handle_cypher_result(_result):
            """
            Handle the results from the cypher result.  If there as no result found then reject the promise, otherwise
            update the session with the uri of the node, and
            :param _result:
            :return:
            """
            if not _result.results:
                raise Exception('No results to work')

            node_uri = _result.results[0].about

            session['node_uri'] = node_uri

            return session

        payload = StoragePayload()
        payload.add_property(key=NEO4J.cypher, value=self.query)
        promise = self._storage_client.execute_cypher(payload).then(_handle_cypher_result)

        return promise

    def _get_interval_properties(self, session):
        """
        Fetch the properties of the the node stored in the session.
        :param session:
        :return:
        """
        def _handle_get_results(_result):
            if not _result.about:
                # This is a weird case, and should never happen
                raise Exception('Node does not exist')

            session['node'] = _result
            return session

        payload = StoragePayload()
        payload.about = session['node_uri']
        promise = self._storage_client.get_node(payload).then(_handle_get_results)

        return promise

    def _get_timelines(self, session):
        """
        Create a promise list that will fetch all of the timelines that the interval should be connected to.
        :param session:
        :return:
        """
        start_date_string = session['node'].properties.get(str(TIMELINE.start), None)
        end_date_string = session['node'].properties.get(str(TIMELINE.end), None)

        start_date_time = isodate.parse_datetime(start_date_string[0])
        start_date = datetime.datetime(start_date_time.year, start_date_time.month, start_date_time.day,
                                       tzinfo=start_date_time.tzinfo)

        if end_date_string:
            end_date_time = isodate.parse_datetime(end_date_string[0])
            end_date = datetime.datetime(end_date_time.year, end_date_time.month, end_date_time.day,
                                         tzinfo=start_date_time.tzinfo)
        else:
            end_date = start_date

        promises = []

        date_difference = end_date - start_date

        for day in range(0, date_difference.days+1):
            delta = datetime.timedelta(days=day)
            timeline_date = start_date + delta

            promise = self._find_or_create_relative_map_for_date(timeline_date)

            promises.append(promise)

        promise = self._scheduler.create_promise_list(*promises).then(
            self._scheduler.generate_promise_handler(self._store_found_timelines, session))

        return promise

    def _store_found_timelines(self, result, session):
        session['timelines'] = result
        return session

    def _find_or_create_relative_map_for_date(self, date):
        """
        Find or create a relative timeline scale that connects to an origin map with the correct date value.
        :param date: date object
        :return:
        """
        def _handle_find_node(_result):
            if not _result.about:
                raise Exception('Result not found')

            return _result

        origin = isodate.isodatetime.datetime_isoformat(date)

        search_payload = StoragePayload()
        search_payload.add_type(TIMELINE.OriginMap)
        search_payload.add_property(TIMELINE.origin, origin)

        promise = self._get_or_create(search_payload)
        promise = promise.then(_handle_find_node)
        promise = promise.then(self._get_range_timeline)

        return promise

    def _get_range_timeline(self, timeline_map):
        """
        Get or create the range timeline for the origin map that was previously found.
        :param timeline_map: the timeline map.
        :return:
        """
        time_lines = timeline_map.references.get(str(TIMELINE.rangeTimeLine), None)
        if time_lines:
            return time_lines[0]

        def _handle_create_and_update_session(_result):
            timeline = _result.results[0].about

            update_payload = StoragePayload()
            update_payload.about = timeline_map.about
            update_payload.add_reference(TIMELINE.rangeTimeLine, timeline)

            # Publish the create event
            self._rdf_publish.publish_all_results(_result, created=True)

            _promise = self._storage_client.update_node(update_payload)
            _promise = _promise.then(self._scheduler.generate_promise_handler(self._rdf_publish.publish_all_results,
                                                                              created=False))
            _promise = _promise.then(lambda s: timeline)

            return _promise

        # otherwise it needs to be created and then return.
        create_payload = StoragePayload()
        create_payload.add_type(TIMELINE.RelativeTimeLine)
        create_payload.add_reference(DCTERMS.creator, self._representation_manager.representation_uri)

        promise = self._storage_client.create_node(create_payload).then(_handle_create_and_update_session)

        return promise

    def _update_interval(self, session):
        """

        Now that the timeline stuff has been found, update the interval as well.
        :param session:
        :return:
        """
        def return_session(result):
            self._rdf_publish.publish_all_results(result, created=False)
            return session

        update_payload = StoragePayload()
        update_payload.about = session['node'].about
        update_payload.add_reference(TIMELINE.timeline, session['timelines'])

        update_payload.add_flag(UpdateFlags.REPLACE_DEFINED, True)

        return self._storage_client.update_node(update_payload).then(return_session)

    def _reschedule(self, session, delay):
        """
        Reschedule the task.
        :param session:
        :param delay:
        :return:
        """
        self._scheduler.schedule_task(self._start_process, delay=delay)

timeline_maintainer = Maintainer
