import logging
import re

from xivo_dao.helpers.db_manager import daosession
from xivo_dao.alchemy.trunkfeatures import TrunkFeatures
from xivo_dao.alchemy.schedule import Schedule
from xivo_dao.alchemy.schedulepath import SchedulePath
from xivo_dao.alchemy.schedule_time import ScheduleTime
from xivo_dao.alchemy.incall import Incall
from xivo_dao.alchemy.extension import Extension
from xivo_dao.alchemy.queue import Queue
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
def get_schedule_from_extension(session, **extension_filters):
    print('get_schedule', extension_filters)
    try:
        # 1. Find Extension with context and type
        ext_query = session.query(Extension).filter_by(**extension_filters)
        ext = ext_query.first()
        if not ext:
            return None
        path_id = ext.typeval
        path = ext.type
        if not path_id or not path:
            return None
        return get_schedule_from_path(session, path, path_id)
    except Exception:
        logger.exception('Failed to get schedules for context %s', context)
        return None


@daosession
def get_schedule_from_path(session, path, pathid):
    try:
        schedule_path = session.query(SchedulePath).filter_by(path=path, pathid=pathid).first()
        print('schedule_path',schedule_path)
        if not schedule_path:
            return None
        print('schedule_path.schedule_id',schedule_path.schedule_id)
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


@daosession
def get_all_queues(session):
    try:
        queues = session.query(Queue).all()
        return queues or []
    except Exception:
        logger.exception('Failed to get all queues')
        return []