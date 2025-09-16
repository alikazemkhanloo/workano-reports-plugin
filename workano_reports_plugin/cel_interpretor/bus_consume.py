import logging
import time

from .cel_interpretor import default_interpretors
from .dao import get_trunk_name_number_map
from .generator import CallLogsGenerator
from wazo_auth_client import Client as AuthClient
from wazo_confd_client import Client as ConfdClient
from .manager import CallLogsManager
from .writer import CallLogsWriter

logger = logging.getLogger(__name__)


class ReportsBusEventHandler:
    def __init__(self, config, dao):
        self.config = config
        self.dao = dao
        auth_client = AuthClient(**config['auth'])
        token = auth_client.token.new(
            expiration=365 * 24 * 60 * 60)['token']

        confd_client = ConfdClient(**config['confd'], token=token)
        trunk_name_number_map = get_trunk_name_number_map()

        generator = CallLogsGenerator(
            confd_client,
            trunk_name_number_map,
            default_interpretors(),
        )
        writer = CallLogsWriter(self.dao)
        self.manager = CallLogsManager(self.dao, generator, writer)



    def subscribe(self, bus_consumer):
        bus_consumer.subscribe('CEL', self.handle_cel_event)

    def handle_cel_event(self, payload):
        if payload['EventName'] != 'LINKEDID_END':
            return

        linked_id = payload['LinkedID']
        start_time = time.time()
        try:
            self.manager.generate_from_linked_id(linked_id)
        except Exception:
            logger.exception(
                'Reports: Failed to generate call log for linkedid "%s"', linked_id
            )
        else:
            processing_time = time.time() - start_time
            logger.info(
                'Reports: Generated call log for linkedid "%s" in %.2fs',
                linked_id,
                processing_time,
            )


