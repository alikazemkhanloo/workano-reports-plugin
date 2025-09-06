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
from wazo_calld_client import Client
from xivo_dao.helpers.db_manager import Session
from wazo_confd_client import Client as ConfdClient
from xivo_dao.helpers.exception import NotFoundError
from . import dao
import logging
# from .notifier import build_campaign_notifier
# from .validator import build_campaign_validator
# from ..campaign_contact_call.model import CampaignContactCallModel
# from ..campaign_contact_call.services import build_campaign_contact_call_service
# from ..contact_list.services import build_contact_list_service

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

