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
from . import dao
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

    def _get_work_hours_from_confd(self, config, tenant):
        """
        Create Auth/Confd clients using config and tenant, fetch schedules and
        return (work_start, work_end) as 'HH:MM' strings from the first schedule item.
        Returns (None, None) if unable to determine.
        """
        if not config or 'auth' not in config:
            return (None, None)

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
            return (None, None)

        # schedules may be an object with 'items' or a list
        if isinstance(schedules, dict) and 'items' in schedules:
            items = schedules['items']
        else:
            items = list(schedules)

        if not items:
            return (None, None)

        first = items[0]
        # Try to extract start/end from known confd schedule schema (wazo confd)
        work_start = None
        work_end = None
        # Known wazo-confd schedule shape uses open_periods[*].hours_start/hours_end
        if isinstance(first, dict):
            open_periods = first.get('open_periods') or []
            if open_periods:
                p = open_periods[0]
                work_start = p.get('hours_start') or p.get('start') or p.get('start_time')
                work_end = p.get('hours_end') or p.get('end') or p.get('end_time')
            else:
                # fallback to exceptional_periods
                exceptional = first.get('exceptional_periods') or []
                if exceptional:
                    p = exceptional[0]
                    work_start = p.get('hours_start') or p.get('start') or p.get('start_time')
                    work_end = p.get('hours_end') or p.get('end') or p.get('end_time')

            # additional fallbacks for older/alternative shapes
            if not (work_start and work_end):
                for k in ('start', 'start_time', 'time_start', 'from'):
                    if k in first and isinstance(first[k], str):
                        work_start = first[k]
                        break
                for k in ('end', 'end_time', 'time_end', 'to'):
                    if k in first and isinstance(first[k], str):
                        work_end = first[k]
                        break
                if not (work_start and work_end):
                    if 'time_ranges' in first and first['time_ranges']:
                        tr = first['time_ranges'][0]
                        work_start = tr.get('start') or tr.get('from') or work_start
                        work_end = tr.get('end') or tr.get('to') or work_end
                    elif 'periods' in first and first['periods']:
                        p = first['periods'][0]
                        work_start = p.get('start_time') or p.get('start') or work_start
                        work_end = p.get('end_time') or p.get('end') or work_end

        # Normalize to HH:MM if datetime present
        def _norm(t):
            if not t:
                return None
            if isinstance(t, str):
                # accept 'HH:MM' or 'HH:MM:SS' or ISO datetime
                if 'T' in t or '+' in t or 'Z' in t:
                    # try isoparse
                    try:
                        dt = _parse_iso_datetime(t)
                        return dt.time().strftime('%H:%M') if dt else None
                    except Exception:
                        return t[:5]
                else:
                    return t[:5]
            if isinstance(t, datetime):
                return t.time().strftime('%H:%M')
            return None

        work_start = _norm(work_start)
        work_end = _norm(work_end)

        return (work_start, work_end)

    def get_reports(self, start_time=None, end_time=None, work_start='09:00', work_end='17:00', config=None, tenant=None):
        """
        Generate reports based on CEL table.
        - start_time / end_time: ISO8601 string or datetime; if None, no bound.
        - work_start / work_end: 'HH:MM' strings defining working hours (inclusive start, exclusive end).
        - config, tenant: if provided, will attempt to fetch schedules from confd and use the first schedule item to set work_start/work_end.

        Returns a dict with totals and breakdown by direction (inbound/outbound/internal)
        and split between calls within working hours and outside working hours.
        """
        # override work hours from confd schedule if available
        if config and tenant:
            try:
                ws, we = self._get_work_hours_from_confd(config, tenant)
                if ws:
                    work_start = ws
                if we:
                    work_end = we
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

