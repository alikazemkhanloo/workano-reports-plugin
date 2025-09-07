import os
import re
import traceback
import time
from datetime import datetime
from threading import Thread
import uuid
import requests
from datetime import datetime, timezone
from marshmallow import ValidationError
from wazo_calld_client import Client as CalldClient
from wazo_auth_client import Client as AuthClient
from wazo_confd_client import Client as ConfdClient
from xivo_dao.helpers.db_manager import Session
from xivo_dao.helpers.exception import NotFoundError
import logging
# from .notifier import build_campaign_notifier
# from .validator import build_campaign_validator
# from ..campaign_contact_call.model import CampaignContactCallModel
# from ..campaign_contact_call.services import build_campaign_contact_call_service
# from ..contact_list.services import build_contact_list_service

from sqlalchemy import func, case, cast, String
from xivo_dao.alchemy.cel import CEL
from xivo_dao.alchemy.schedule import Schedule
try:
    from dateutil import parser as _dateutil_parser
except Exception:
    _dateutil_parser = None
from datetime import time as dt_time

try:
    from zoneinfo import ZoneInfo
except Exception:
    try:
        from dateutil import tz as _dateutil_tz
        ZoneInfo = None
    except Exception:
        _dateutil_tz = None
        ZoneInfo = None


def _parse_iso_datetime(s):
    if not s:
        return None
    if isinstance(s, datetime):
        return s
    if _dateutil_parser:
        return _dateutil_parser.isoparse(s)
    # Fallback to fromisoformat (may be limited for some ISO variants)
    try:
        return datetime.fromisoformat(s)
    except Exception:
        # last-resort: try simple parsing
        try:
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S%z")
        except Exception:
            return None


def _parse_time_hhmm(s):
    if not s:
        return None
    parts = s.split(":")
    try:
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        return dt_time(h, m)
    except Exception:
        return None


def _is_dt_in_period(dt_obj, period):
    """Return True if dt_obj (aware datetime) falls into period.
    period keys: hours_start, hours_end, week_days (list of 1-7), month_days, months, timezone
    """
    tzname = period.get('timezone')
    # convert datetime to period timezone if provided
    try:
        if tzname:
            if ZoneInfo:
                tz = ZoneInfo(tzname)
                dt_local = dt_obj.astimezone(tz)
            elif _dateutil_tz:
                tz = _dateutil_tz.gettz(tzname)
                if tz:
                    dt_local = dt_obj.astimezone(tz)
                else:
                    dt_local = dt_obj
            else:
                dt_local = dt_obj
        else:
            dt_local = dt_obj
    except Exception:
        dt_local = dt_obj

    week_day = dt_local.isoweekday()  # 1..7 Monday..Sunday
    month = dt_local.month
    month_day = dt_local.day

    months = set(period.get('months') or [])
    if months and month not in months:
        return False
    month_days = set(period.get('month_days') or [])
    if month_days and month_day not in month_days:
        return False
    week_days = set(period.get('week_days') or [])
    if week_days and week_day not in week_days:
        return False

    # compare times
    start_s = period.get('hours_start')
    end_s = period.get('hours_end')
    start_t = _parse_time_hhmm(start_s) if isinstance(start_s, str) else _parse_time_hhmm(str(start_s))
    end_t = _parse_time_hhmm(end_s) if isinstance(end_s, str) else _parse_time_hhmm(str(end_s))
    if not start_t or not end_t:
        return False

    cur_t = dt_local.time()
    # Handle same-day interval (start <= t < end)
    if start_t <= cur_t < end_t:
        return True
    # Handle overnight intervals where end <= start (e.g., 22:00-06:00)
    if end_t <= start_t:
        if cur_t >= start_t or cur_t < end_t:
            return True
    return False


logger = logging.getLogger(__name__)
UPLOAD_FOLDER = '/var/lib/wazo/sounds/tenants'  # Make sure this directory exists and is writable
TMP_UPLOAD_FOLDER = '/var/lib/wazo/sounds/tmp'  # Make sure this directory exists and is writable
TTS_UPLOAD_FOLDER = '/var/lib/wazo/sounds/tts'  # Make sure this directory exists and is writable


def build_otp_request_service(dao):
    return WorkanoReportsService(dao)


class WorkanoReportsService:

    def __init__(self, dao):
        self.dao = dao
        super().__init__()

    def _parse_time(self, tstr):
        """Parse a time string 'HH:MM' into a datetime.time object."""
        if isinstance(tstr, dt_time):
            return tstr
        if not tstr:
            return None
        if isinstance(tstr, str):
            parts = tstr.split(":")
            if len(parts) >= 2:
                return dt_time(int(parts[0]), int(parts[1]))
        return None

    def _get_work_hours_from_confd(self, config, tenant, schedule_id=None):
        """
        Read schedules from the database using xivo-dao models instead of calling confd.
        Returns a dict with 'open_periods' and 'exceptional_periods' lists extracted from the selected schedule.
        """
        # Use DB session to get schedule
        session = Session()
        try:
            q = session.query(Schedule)
            schedule = None
            if schedule_id is not None:
                try:
                    sid = int(schedule_id)
                except Exception:
                    sid = None
                if sid is not None:
                    schedule = q.filter(Schedule.id == sid).first()
            if schedule is None:
                if tenant:
                    schedule = q.filter(Schedule.tenant_uuid == tenant).order_by(Schedule.id).first()
                else:
                    schedule = q.order_by(Schedule.id).first()

            if not schedule:
                return {}

            tz = schedule.timezone
            periods = {'open_periods': [], 'exceptional_periods': []}

            # schedule.open_periods and exceptional_periods are provided by the model
            for p in getattr(schedule, 'open_periods', []) or []:
                try:
                    periods['open_periods'].append({
                        'hours_start': getattr(p, 'hours_start', None),
                        'hours_end': getattr(p, 'hours_end', None),
                        'week_days': getattr(p, 'week_days', []) or [],
                        'month_days': getattr(p, 'month_days', []) or [],
                        'months': getattr(p, 'months_list', []) or [],
                        'timezone': tz,
                    })
                except Exception:
                    continue

            for p in getattr(schedule, 'exceptional_periods', []) or []:
                try:
                    periods['exceptional_periods'].append({
                        'hours_start': getattr(p, 'hours_start', None),
                        'hours_end': getattr(p, 'hours_end', None),
                        'week_days': getattr(p, 'week_days', []) or [],
                        'month_days': getattr(p, 'month_days', []) or [],
                        'months': getattr(p, 'months_list', []) or [],
                        'timezone': tz,
                    })
                except Exception:
                    continue

            # Normalize hours format to ensure HH:MM strings
            for listname in ('open_periods', 'exceptional_periods'):
                for per in periods.get(listname, []):
                    if isinstance(per.get('hours_start'), str):
                        per['hours_start'] = per['hours_start'][:5]
                    if isinstance(per.get('hours_end'), str):
                        per['hours_end'] = per['hours_end'][:5]

            return periods
        finally:
            try:
                session.close()
            except Exception:
                pass

    def get_reports(self, params, config=None, tenant=None):
        """
        Generate reports based on CEL table.
        - start_time / end_time: ISO8601 string or datetime; if None, no bound.
        - config, tenant: if provided, will attempt to fetch schedules from DB and use the selected schedule to determine working periods.

        Returns a dict with totals and breakdown by direction (inbound/outbound/internal)
        and split between calls within working hours and outside working hours.
        """
        start_time=params.get('start_time')
        end_time=params.get('end_time')
        schedule_id=params.get('schedule_id')
        # override work hours from confd schedule if available
        schedule_periods = None

        if config and tenant:
            try:
                schedule_periods = self._get_work_hours_from_confd(config, tenant, schedule_id=schedule_id)
            except Exception:
                logger.exception('Failed to fetch schedule from confd')

        # parse datetimes
        if isinstance(start_time, str):
            start_time = _parse_iso_datetime(start_time)
        if isinstance(end_time, str):
            end_time = _parse_iso_datetime(end_time)

        # No simple work_start/work_end fallback: when no schedule periods are available,
        # calls will be considered outside working hours.

        session = Session()
        try:
            # Fallback to scanning all CEL rows and grouping in Python (groupby produced incorrect trunk attribution).
            q = session.query(CEL)
            if start_time:
                q = q.filter(CEL.eventtime >= start_time)
            if end_time:
                q = q.filter(CEL.eventtime <= end_time)

            # build calls dict keyed by linkedid/uniqueid/id
            calls = {}
            for cel in q:
                lid = cel.linkedid or cel.uniqueid or str(cel.id)
                entry = calls.get(lid)
                if not entry:
                    entry = {
                        'first_event': cel.eventtime,
                        'eventtypes': set(),
                        'channame': cel.channame,
                        'did_cid_dnid': None,
                        'did_channame': None,
                    }
                    calls[lid] = entry
                else:
                    # update earliest event
                    if cel.eventtime and entry.get('first_event'):
                        if cel.eventtime < entry['first_event']:
                            entry['first_event'] = cel.eventtime
                    elif cel.eventtime:
                        entry['first_event'] = cel.eventtime
                    # keep a representative channame when not set
                    if not entry.get('channame') and getattr(cel, 'channame', None):
                        entry['channame'] = cel.channame

                if cel.eventtype:
                    entry['eventtypes'].add(cel.eventtype)

                # capture did-specific info when present
                try:
                    if getattr(cel, 'context', None) == 'did' and getattr(cel, 'cid_dnid', None):
                        # prefer cid_dnid as trunk identifier
                        entry['did_cid_dnid'] = cel.cid_dnid
                        entry['did_channame'] = cel.channame
                except Exception:
                    pass

            # initialize counters (same shape as before)
            result = {
                'total': {'working_hours': 0, 'outside_working_hours': 0, 'total': 0, 'by_trunk': {}},
                'by_direction': {
                    'inbound': {'working_hours': 0, 'outside_working_hours': 0, 'total': 0, 'by_trunk': {}},
                    'outbound': {'working_hours': 0, 'outside_working_hours': 0, 'total': 0, 'by_trunk': {}},
                    'internal': {'working_hours': 0, 'outside_working_hours': 0, 'total': 0, 'by_trunk': {}},
                },
                'by_trunk': {},
            }

            for lid, info in calls.items():
                start_evt = info.get('first_event')
                eventtypes = info.get('eventtypes', set())

                # determine direction
                if 'XIVO_INCALL' in eventtypes or 'xivo_incall' in eventtypes:
                    direction = 'inbound'
                elif 'XIVO_OUTCALL' in eventtypes or 'xivo_outcall' in eventtypes:
                    direction = 'outbound'
                else:
                    direction = 'internal'

                # derive trunk: prefer did_cid_dnid, else try did_channame, else channame
                trunk = None
                try:
                    if info.get('did_cid_dnid'):
                        trunk = str(info['did_cid_dnid'])
                    else:
                        chan = info.get('did_channame') or info.get('channame') or ''
                        m = re.match(r'^[^/]+/([^\-;:@]+)', chan)
                        if m:
                            trunk = m.group(1)
                except Exception:
                    trunk = None

                # ensure trunk containers
                if trunk and trunk not in result['by_trunk']:
                    result['by_trunk'][trunk] = {
                        'total': {'working_hours': 0, 'outside_working_hours': 0, 'total': 0},
                        'by_direction': {
                            'inbound': {'working_hours': 0, 'outside_working_hours': 0, 'total': 0},
                            'outbound': {'working_hours': 0, 'outside_working_hours': 0, 'total': 0},
                            'internal': {'working_hours': 0, 'outside_working_hours': 0, 'total': 0},
                        },
                    }
                if trunk and trunk not in result['total']['by_trunk']:
                    result['total']['by_trunk'][trunk] = {'working_hours': 0, 'outside_working_hours': 0, 'total': 0}
                if trunk and trunk not in result['by_direction'][direction]['by_trunk']:
                    result['by_direction'][direction]['by_trunk'][trunk] = {'working_hours': 0, 'outside_working_hours': 0, 'total': 0}

                # determine if within working hours
                in_work = False
                if start_evt:
                    if schedule_periods:
                        opens = schedule_periods.get('open_periods', [])
                        excs = schedule_periods.get('exceptional_periods', [])
                        in_open = False
                        for per in opens:
                            try:
                                if _is_dt_in_period(start_evt, per):
                                    in_open = True
                                    break
                            except Exception:
                                continue
                        in_exception = False
                        for per in excs:
                            try:
                                if _is_dt_in_period(start_evt, per):
                                    in_exception = True
                                    break
                            except Exception:
                                continue
                        in_work = in_open and (not in_exception)
                    else:
                        in_work = False

                if in_work:
                    result['total']['working_hours'] += 1
                    result['by_direction'][direction]['working_hours'] += 1
                    if trunk:
                        result['total']['by_trunk'][trunk]['working_hours'] += 1
                        result['by_direction'][direction]['by_trunk'][trunk]['working_hours'] += 1
                        result['by_trunk'][trunk]['total']['working_hours'] += 1
                        result['by_trunk'][trunk]['by_direction'][direction]['working_hours'] += 1
                else:
                    result['total']['outside_working_hours'] += 1
                    result['by_direction'][direction]['outside_working_hours'] += 1
                    if trunk:
                        result['total']['by_trunk'][trunk]['outside_working_hours'] += 1
                        result['by_direction'][direction]['by_trunk'][trunk]['outside_working_hours'] += 1
                        result['by_trunk'][trunk]['total']['outside_working_hours'] += 1
                        result['by_trunk'][trunk]['by_direction'][direction]['outside_working_hours'] += 1

                result['total']['total'] += 1
                result['by_direction'][direction]['total'] += 1
                if trunk:
                    result['total']['by_trunk'][trunk]['total'] += 1
                    result['by_direction'][direction]['by_trunk'][trunk]['total'] += 1
                    result['by_trunk'][trunk]['total']['total'] += 1
                    result['by_trunk'][trunk]['by_direction'][direction]['total'] += 1

            return result
        finally:
            try:
                session.close()
            except Exception:
                pass

