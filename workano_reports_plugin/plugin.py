import logging
from wazo_calld_client import Client as CalldClient
from wazo_auth_client import Client as AuthClient
from wazo_confd_client import Client as ConfdClient
from .db import init_db
from .services import build_otp_request_service
from .bus_consume import OtpBusEventHandler
from .resource import OtpFileUploadResource, OtpPlaybackResource, OtpReportItemResource, OtpReportResource
logger = logging.getLogger(__name__)

class Plugin:
    def load(self, dependencies):
        logger.info('otp request plugin loading')
        api = dependencies['api']
        config = dependencies['config']
        otp_config = config['otp-plugin']
        token = otp_config['token']
        tenant = otp_config['tenant']
        auth_client = AuthClient(**config['auth'])
        # token = auth_client.token.new(expiration=365 * 24 * 60 * 60, username=config['auth']['username'], password=config['auth']['password'])['token']
        # token = config['auth']['password']

        calld_client = CalldClient(host='127.0.0.1', port=443, verify_certificate=False, https=True, token=token, tenant=tenant)
        confd_client = ConfdClient(host='127.0.0.1', port=443, verify_certificate=False, https=True, token=token, tenant=tenant)
        init_db('postgresql://asterisk:proformatique@localhost/asterisk?application_name=workano-otp-plugin')
        otp_request_service = build_otp_request_service(auth_client, calld_client, confd_client, otp_config)
        bus_consumer = dependencies['bus_consumer']
        bus_event_handler = OtpBusEventHandler(otp_request_service)

        # Subscribe to bus events
        bus_event_handler.subscribe(bus_consumer)

        # Campaigns
        api.add_resource(
            OtpPlaybackResource,
            '/otp',
            endpoint='create_otp',
            resource_class_args=(otp_request_service,)
        )

        api.add_resource(
            OtpFileUploadResource,
            '/otp/upload',
            resource_class_args=(otp_request_service,)
        )
        api.add_resource(
            OtpReportResource,
            '/otp/results',
            resource_class_args=(otp_request_service,)
        )
        api.add_resource(
            OtpReportItemResource,
            '/otp/results/<id>',
            resource_class_args=(otp_request_service,)
        )
    def unload(self):
        pass
