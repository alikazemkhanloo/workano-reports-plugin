import logging
import re

from xivo_dao.helpers.db_manager import daosession
from xivo_dao.alchemy.trunkfeatures import TrunkFeatures
from xivo_dao.alchemy.schedule import Schedule
from xivo_dao.alchemy.schedulepath import SchedulePath
from xivo_dao.alchemy.schedule_time import ScheduleTime
from xivo_dao.alchemy.incall import Incall
from xivo_dao.alchemy.extension import Extension
from sqlalchemy.orm import selectinload

logger = logging.getLogger(__name__)

@daosession
def get_trunk_name_number_map(session):
    """Build and return a mapping {trunk_name: number} from database.

    Extracts the user part from the 'contact' value in aor_section_options 
    of the associated EndpointSIP.
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


@daosession
def get_schedule(session, context, type, exten):
    """
    Find the schedule related to the given context.
    1. Find Extension with context and type.
    2. Get typeval (incall id).
    3. Find the first SchedulePath where path=type and pathid=incall.id (unique).
    4. Return the related Schedule or None. Preload schedule periods (ScheduleTime) via selectinload.
    """
    print('get_schedule', context, type, exten)
    try:
        # 1. Find Extension with context and type
        ext_query = session.query(Extension).filter_by(context=context, type=type)
        if exten is not None:
            ext_query = ext_query.filter_by(exten=exten)
        ext = ext_query.first()
        if not ext:
            return None
        path_id = ext.typeval
        path = ext.type
        if not path_id or not path:
            return None
        # 2. Find the first SchedulePath where path='path' and pathid=path_id
        schedule_path = session.query(SchedulePath).filter_by(path=path, pathid=path_id).first()
        print('schedule_path',schedule_path)
        if not schedule_path:
            return None
        print('schedule_path.schedule_id',schedule_path.schedule_id)
        # 3. Get related Schedule and preload periods
        schedule = (
            session.query(Schedule)
            .options(selectinload(Schedule.periods))
            .filter_by(id=schedule_path.schedule_id)
            .first()
        )
        return schedule
    except Exception:
        logger.exception('Failed to get schedules for context %s', context)
        return None
