from sqlalchemy import (
    Column
)
from sqlalchemy import text
from sqlalchemy.types import (String, Boolean, DateTime, Text, Integer)
from xivo_dao.helpers.db_manager import UUIDAsString
from sqlalchemy.types import JSON

from .db import Base

class OtpModel(Base):
    __tablename__ = 'plugin_otp_playback_request'
    uuid = Column(UUIDAsString(36), primary_key=True,
                  server_default=text('uuid_generate_v4()'))
    call_id = Column(String(128), nullable=False)
    tenant_uuid = Column(UUIDAsString(36), nullable=False)
    application_uuid = Column(UUIDAsString(36), nullable=False)
    number = Column(String(128), nullable=False)
    caller_id_name = Column(String(128), nullable=False)
    caller_id_number = Column(String(128), nullable=False)
    language = Column(String(128), nullable=False)
    uris = Column(JSON, nullable=True)
    status = Column(String(128), nullable=False)
    creation_time = Column(DateTime(timezone=True), nullable=False)
    end_time = Column(DateTime(timezone=True), nullable=True)
    answer_time = Column(DateTime(timezone=True), nullable=True)
    talking_to = Column(JSON, nullable=True)
    file_name = Column(String(45), nullable=True)
    tts = Column(Text, nullable=True)
    end_call_reason = Column(Integer, nullable = True)
