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

from sqlalchemy import func
from xivo_dao.alchemy.cel import CEL
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
        Create Auth/Confd clients using config and tenant, fetch schedules and
        return a dict with 'open_periods' and 'exceptional_periods' lists extracted from the first schedule item.
        Each period dict contains: hours_start, hours_end, week_days, month_days, months, timezone
        Returns empty dict if unable to determine.
        """
        if not config or 'auth' not in config:
            return {}

        # Build auth client
        try:
            auth_client = AuthClient(**config['auth'])
        except Exception:
            auth_client = None

        token = None
        try:
            if auth_client and hasattr(auth_client, 'token'):
                # Try to create a long-lived token if API available
                try:
                    token = auth_client.token.new(expiration=365 * 24 * 60 * 60,
                                                  username=config['auth'].get('username'),
                                                  password=config['auth'].get('password'))
                    if isinstance(token, dict) and 'token' in token:
                        token = token['token']
                except Exception:
                    # fallback: if password is already token
                    token = config['auth'].get('password')
            else:
                token = config['auth'].get('password')
        except Exception:
            token = config['auth'].get('password')

        host = config.get('host', '127.0.0.1')
        port = config.get('port', 443)
        verify_certificate = config.get('verify_certificate', False)
        https = config.get('https', True)

        # Create clients
        try:
            calld_client = CalldClient(host=host, port=port,
                                       verify_certificate=verify_certificate, https=https,
                                       token=token, tenant=tenant)
        except Exception:
            calld_client = None

        try:
            confd_client = ConfdClient(host=host, port=port,
                                       verify_certificate=verify_certificate, https=https,
                                       token=token, tenant=tenant)
        except Exception:
            confd_client = None

        schedules = None
        if confd_client:
            try:
                schedules = confd_client.schedules.list()
            except Exception:
                logger.exception('Failed to fetch schedules via confd_client.schedules.list()')
                schedules = None

        if not schedules:
            return {}

        # schedules may be an object with 'items' or a list
        if isinstance(schedules, dict) and 'items' in schedules:
            items = schedules['items']
        else:
            try:
                items = list(schedules)
            except Exception:
                items = []

        if not items:
            return {}

        # If schedule_id provided, select the schedule with matching id from the list; otherwise pick the first
        first = None
        if schedule_id is not None:
            try:
                sid = int(schedule_id)
            except Exception:
                sid = None

            if sid is not None:
                for itm in items:
                    iid = None
                    if isinstance(itm, dict):
                        iid = itm.get('id')
                    else:
                        iid = getattr(itm, 'id', None)

                    if iid == sid:
                        first = itm
                        break
        logger.debug('>>>first',first)
        if first is None:
            first = items[0]

        periods = {'open_periods': [], 'exceptional_periods': []}
        if isinstance(first, dict):
            tz = first.get('timezone')
            # open_periods
            for p in (first.get('open_periods') or []):
                periods['open_periods'].append({
                    'hours_start': p.get('hours_start'),
                    'hours_end': p.get('hours_end'),
                    'week_days': p.get('week_days') or [],
                    'month_days': p.get('month_days') or [],
                    'months': p.get('months') or [],
                    'timezone': tz,
                })
            # exceptional_periods (they are exceptions to open_periods)
            for p in (first.get('exceptional_periods') or []):
                periods['exceptional_periods'].append({
                    'hours_start': p.get('hours_start'),
                    'hours_end': p.get('hours_end'),
                    'week_days': p.get('week_days') or [],
                    'month_days': p.get('month_days') or [],
                    'months': p.get('months') or [],
                    'timezone': tz,
                })

        # Normalize hours format to ensure HH:MM strings
        for listname in ('open_periods', 'exceptional_periods'):
            for per in periods.get(listname, []):
                if isinstance(per.get('hours_start'), str):
                    per['hours_start'] = per['hours_start'][:5]
                if isinstance(per.get('hours_end'), str):
                    per['hours_end'] = per['hours_end'][:5]

        return periods

    def get_reports(self, start_time=None, end_time=None, work_start='09:00', work_end='17:00', config=None, tenant=None, schedule_id=None):
        """
        Generate reports based on CEL table.
        - start_time / end_time: ISO8601 string or datetime; if None, no bound.
        - work_start / work_end: 'HH:MM' strings defining working hours (inclusive start, exclusive end).
        - config, tenant: if provided, will attempt to fetch schedules from confd and use the first schedule item to set work_start/work_end.

        Returns a dict with totals and breakdown by direction (inbound/outbound/internal)
        and split between calls within working hours and outside working hours.
        """
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

        work_start_t = self._parse_time(work_start) or dt_time(9, 0)
        work_end_t = self._parse_time(work_end) or dt_time(17, 0)

        session = Session()
        try:
            q = session.query(CEL)
            if start_time:
                q = q.filter(CEL.eventtime >= start_time)
            if end_time:
                q = q.filter(CEL.eventtime <= end_time)

            # iterate cels and group by linkedid (fallback to uniqueid)
            calls = {}
            for cel in q:
                lid = cel.linkedid or cel.uniqueid or str(cel.id)
                entry = calls.get(lid)
                if not entry:
                    entry = {
                        'first_event': cel.eventtime,
                        'eventtypes': set(),
                    }
                    calls[lid] = entry
                else:
                    if cel.eventtime and entry['first_event']:
                        if cel.eventtime < entry['first_event']:
                            entry['first_event'] = cel.eventtime
                if cel.eventtype:
                    entry['eventtypes'].add(cel.eventtype)

            # initialize counters
            result = {
                'total': {'working_hours': 0, 'outside_working_hours': 0, 'total': 0},
                'by_direction': {
                    'inbound': {'working_hours': 0, 'outside_working_hours': 0, 'total': 0},
                    'outbound': {'working_hours': 0, 'outside_working_hours': 0, 'total': 0},
                    'internal': {'working_hours': 0, 'outside_working_hours': 0, 'total': 0},
                },
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

                # determine if within working hours
                in_work = False
                if start_evt:
                    # If schedule periods are present, test against them (open_periods minus exceptional_periods)
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
                        # fallback to simple daily time window
                        local_time = start_evt.timetz() if hasattr(start_evt, 'timetz') else start_evt.time()
                        # Normalize to naive time comparators (hours/min)
                        st = dt_time(local_time.hour, local_time.minute, local_time.second)
                        if work_start_t <= st < work_end_t:
                            in_work = True

                if in_work:
                    result['total']['working_hours'] += 1
                    result['by_direction'][direction]['working_hours'] += 1
                else:
                    result['total']['outside_working_hours'] += 1
                    result['by_direction'][direction]['outside_working_hours'] += 1

                result['total']['total'] += 1
                result['by_direction'][direction]['total'] += 1

            return result
        finally:
            try:
                session.close()
            except Exception:
                pass

