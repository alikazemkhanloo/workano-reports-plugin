import logging
import time
import re

from workano_reports_plugin.cel_interpretor import default_interpretors
from workano_reports_plugin.generator import CallLogsGenerator
from wazo_auth_client import Client as AuthClient
from wazo_confd_client import Client as ConfdClient
from xivo.token_renewer import TokenRenewer
from xivo_dao.helpers.db_manager import daosession
from xivo_dao.alchemy.trunkfeatures import TrunkFeatures
from workano_reports_plugin.manager import CallLogsManager
from workano_reports_plugin.writer import CallLogsWriter

logger = logging.getLogger(__name__)


@daosession
def get_trunk_name_number_map(session):
    """Build and return a mapping {trunk_name: number} from database.

    Uses the same logic as `_find_number_from_trunk_db` to extract the user part
    from the 'contact' value in aor_section_options of the associated EndpointSIP.
    Only entries with both a name and a parsed number are included.
    """
    mapping = {}

    try:
        # iterate over TrunkFeatures
        trunks = session.query(TrunkFeatures).all()
        for t in trunks or []:
            try:
                name = getattr(t, 'name', None) or getattr(getattr(t, 'endpoint_sip', None), 'name', None)
                number = None

                ep = getattr(t, 'endpoint_sip', None)
                if ep:
                    for opt in getattr(ep, 'aor_section_options', []) or []:
                        if isinstance(opt, (list, tuple)) and len(opt) >= 2 and opt[0] == 'contact':
                            m = re.match(r'sip:(.*)@', opt[1])
                            if m:
                                number = m.group(1)
                                break

                if name and number:
                    mapping[name] = number
            except Exception:
                # Best-effort; skip problematic entries
                continue

    except Exception:
        logger.exception('Failed to build trunk name->number map')

    return mapping

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


