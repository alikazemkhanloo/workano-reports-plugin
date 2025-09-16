import logging

from workano_reports_plugin.cdrpro.resource import CDRListResource
from workano_reports_plugin.cdrpro.services import build_cdr_pro_service
from workano_reports_plugin.cel_interpretor.bus_consume import ReportsBusEventHandler
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
        cdr_pro_service = build_cdr_pro_service()

        # survey
        api.add_resource(
            CDRListResource,
            '/cdr-pro',
            resource_class_args=(cdr_pro_service,)
        )
