import logging

from workano_reports_plugin.bus_consume import ReportsBusEventHandler
from workano_reports_plugin.db import init_db
from .services import build_otp_request_service
from .resource import  ReportsResource
logger = logging.getLogger(__name__)

class Plugin:
    def load(self, dependencies):
        logger.info('workano reports plugin loading')
        api = dependencies['api']
        dao = dependencies['dao']
        config = dependencies['config']
        bus_consumer = dependencies['bus_consumer']
        init_db(config['db_uri'])
        otp_request_service = build_otp_request_service(dao)
        bus_event_handler = ReportsBusEventHandler(config, dao)

        # Subscribe to bus events
        bus_event_handler.subscribe(bus_consumer)

        api.add_resource(
            ReportsResource,
            '/reports',
            resource_class_args=(otp_request_service, config)
        )
