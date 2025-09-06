import logging

logger = logging.getLogger(__name__)


class OtpBusEventHandler:
    def __init__(self, service):
        self._service = service

    def subscribe(self, bus_consumer):
        pass
        # bus_consumer.subscribe('application_call_answered', self._application_call_answered)
        # bus_consumer.subscribe('application_playback_deleted', self._application_playback_deleted)
        # bus_consumer.subscribe('application_call_deleted', self._application_call_deleted)
        # bus_consumer.subscribe('application_playback_created', self._application_playback_created)
        # bus_consumer.subscribe('application_call_entered', self._application_call_entered)
        # bus_consumer.subscribe('application_call_initiated', self._application_call_initiated)
        # bus_consumer.subscribe('application_call_updated', self._application_call_updated)
        # bus_consumer.subscribe('application_progress_started', self._application_progress_started)
        # bus_consumer.subscribe('application_progress_stopped', self._application_progress_stopped)
        # bus_consumer.subscribe('application_destination_node_created', self._application_destination_node_created)
        # bus_consumer.subscribe('application_node_created', self._application_node_created)
        # bus_consumer.subscribe('application_node_deleted', self._application_node_deleted)
        # bus_consumer.subscribe('application_node_updated', self._application_node_updated)
        # bus_consumer.subscribe('application_user_outgoing_call_created', self._application_user_outgoing_call_created)
        # bus_consumer.subscribe('call_ended', self._call_ended)
        # bus_consumer.subscribe('call_answered', self._call_answered)
        # bus_consumer.subscribe('call_created', self._call_created)
        # bus_consumer.subscribe('call_updated', self._call_updated)

    def _application_call_answered(self, event):
        logger.warning('========>application_call_answered<===========')
        logger.warning(event)
        self._service.application_call_answered(event)

    # def _application_playback_created(self, event):
    #     logger.warning('========>application_playback_created<===========')
    #     logger.warning(event)

    #     self._service.application_playback_created(event)

    def _application_playback_deleted(self, event):
        logger.warning('========>application_playback_deleted<===========')
        logger.warning(event)
        # {'application_uuid':'7d48038a-fede-4554-8ae0-1d0df26ff232',
        # 'playback':{'uri':'sound:/var/lib/wazo/sounds/tenants/501ec54b-aa48-4492-bc5c-7af59c20705f/campaign/campagin_test',
        # 'uuid': 'c3a0beff-67f8-4297-8fed-fac9fa3c6728', 'language': 'en'}}
        self._service.application_playback_deleted(event)

    def _application_call_deleted(self, event):
        logger.warning('========>application_call_deleted<===========')
        logger.warning(event)
        # {'application_uuid': '7d48038a-fede-4554-8ae0-1d0df26ff232', 'call': {'id': '1661578551.87', 'moh_uuid': None,
        # 'caller_id_number': '100', 'tenant_uuid': None, 'dialed_extension': None, 'node_uuid': None, 'muted': False,
        # 'snoops': {}, 'status': 'Up', 'creation_time': '2022-08-27T01:35:51.041-0400', 'caller_id_name': 'test1',
        # 'user_uuid': None, 'is_caller': False, 'on_hold': False}}
        self._service.application_call_deleted(event)

    # def _application_call_entered(self, event):
    #     logger.warning('========>application_call_entered<===========')
    #     logger.warning(event)

    # def _application_call_initiated(self, event):
    #     logger.warning('========>application_call_initiated<===========')
    #     logger.warning(event)

    # def _application_call_updated(self, event):
    #     logger.warning('========>application_call_updated<===========')
    #     logger.warning(event)

    # def _application_progress_started(self, event):
    #     logger.warning('========>application_progress_started<===========')
    #     logger.warning(event)

    # def _application_progress_stopped(self, event):
    #     logger.warning('========>application_progress_stopped<===========')
    #     logger.warning(event)

    # def _application_destination_node_created(self, event):
    #     logger.warning('========>application_destination_node_created<===========')
    #     logger.warning(event)

    # def _application_node_created(self, event):
    #     logger.warning('========>application_node_created<===========')
    #     logger.warning(event)

    # def _application_node_deleted(self, event):
    #     logger.warning('========>application_node_deleted<===========')
    #     logger.warning(event)

    # def _application_node_updated(self, event):
    #     logger.warning('========>application_node_updated<===========')
    #     logger.warning(event)

    # def _application_user_outgoing_call_created(self, event):
    #     logger.warning('========>application_user_outgoing_call_created<===========')
    #     logger.warning(event)

    def _call_ended(self, event):
        # {'sip_call_id': 'f3d65b6b-3b49-4ebb-8071-c2a632977d07', 'talking_to': {}, 'status': 'Ringing',
        # 'hangup_time': '2022-09-06T03:17:04.873484-04:00', 'conversation_id': '1662448594.20', 'is_video': False,
        # 'record_state': 'inactive', 'muted': False, 'answer_time': None, 'on_hold': False,
        # 'user_uuid': 'e43dc53d-7aea-47b0-9891-f5e18e9a1e01', 'creation_time': '2022-09-06T03:16:34.672-0400',
        # 'is_caller': False, 'caller_id_number': '161', 'bridges': [], 'call_id': '1662448594.22', 'line_id': 2,
        # 'dialed_extension': '101', 'peer_caller_id_name': 'test2', 'peer_caller_id_number': '101',
        # 'caller_id_name': 'Milad Razban', 'reason_code': 16}
        logger.warning('original========>call_ended<===========')
        logger.warning(event)
        self._service.call_ended(event)
    # def _call_created(self, event):
    #     logger.warning('========>call_created<===========')
    #     logger.warning(event)

    # def _call_answered(self, event):
    #     self._service.call_answered(event)
    # def _call_updated(self, event):
    #     logger.warning('========>call_updated<===========')
    #     logger.warning(event)
