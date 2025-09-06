import logging
from .services import build_otp_request_service
from .resource import  ReportsResource
logger = logging.getLogger(__name__)

class Plugin:
    def load(self, dependencies):
        logger.info('otp request plugin loading')
        api = dependencies['api']
        dao = dependencies['dao']
        config = dependencies['config']
        otp_request_service = build_otp_request_service(dao)

        api.add_resource(
            ReportsResource,
            '/reports',
            resource_class_args=(otp_request_service, config)
        )
