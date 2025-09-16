# Copyright 2021-2025 The Wazo Authors  (see the AUTHORS file)
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

from datetime import timedelta as td
from datetime import timezone as tz

from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import relationship
from sqlalchemy.schema import CheckConstraint, Column, ForeignKey, Index
from sqlalchemy.sql import and_, case, select, text
from sqlalchemy.types import Boolean, DateTime, Enum, Integer, String, Text, JSON
from sqlalchemy_utils import UUIDType, generic_repr
from ..db import Base


@generic_repr
class Tenant(Base):
    __tablename__ = 'call_logd_tenant'

    uuid = Column(UUIDType, primary_key=True)


@generic_repr
class ReportsCallLog(Base):
    __tablename__ = 'plugin_reports_call_log'

    id = Column(Integer, nullable=False, primary_key=True)
    date = Column(DateTime(timezone=True), nullable=False)
    date_answer = Column(DateTime(timezone=True))
    date_end = Column(DateTime(timezone=True))
    tenant_uuid = Column(
        UUIDType,
        ForeignKey(
            'call_logd_tenant.uuid',
            name='plugin_reports_call_log_tenant_uuid_fkey',
            ondelete='CASCADE',
        ),
        nullable=False,
    )
    source_name = Column(String(255))
    source_exten = Column(String(255))
    source_internal_name = Column(Text)
    source_internal_exten = Column(Text)
    source_internal_context = Column(Text)
    source_line_identity = Column(String(255))
    requested_name = Column(Text)
    requested_exten = Column(String(255))
    requested_context = Column(String(255))
    requested_internal_exten = Column(Text)
    requested_internal_context = Column(Text)
    # History of IVR choices made during the call. Stored as a JSON array of objects
    # each object can contain keys like id, exten, context, channame, eventtime.
    ivr_choices = Column(JSON, nullable=True)
    destination_name = Column(String(255))
    destination_exten = Column(String(255))
    destination_internal_exten = Column(Text)
    destination_internal_context = Column(Text)
    destination_line_identity = Column(String(255))
    blocked = Column(Boolean)
    direction = Column(String(255))
    # Optional trunk identifier for the call (e.g. SIP/PJSIP trunk name)
    trunk = Column(String(255))
    user_field = Column(String(255))
    conversation_id = Column(String(255))
    schedule_state = Column(JSON, nullable=True)
    original_call_log_id = Column(Integer, nullable=True)
    recordings = relationship(
        'ReportsRecording',
        order_by='ReportsRecording.start_time',
        cascade='all,delete-orphan',
    )
    participants = relationship('ReportsCallLogParticipant', cascade='all,delete-orphan')
    participant_user_uuids = association_proxy('participants', 'user_uuid')

    source_participant = relationship(
        'ReportsCallLogParticipant',
        primaryjoin='''and_(
            ReportsCallLogParticipant.call_log_id == ReportsCallLog.id,
            ReportsCallLogParticipant.role == 'source'
        )''',
        viewonly=True,
        uselist=False,
    )
    source_user_uuid = association_proxy('source_participant', 'user_uuid')
    source_line_id = association_proxy('source_participant', 'line_id')

    destination_details = relationship(
        'ReportsDestination',
        primaryjoin='''and_(
            ReportsDestination.call_log_id == ReportsCallLog.id,
        )''',
        uselist=True,
        cascade='all,delete-orphan',
        passive_deletes=True,
        lazy='subquery',
    )

    # Relationship to normalized forwards table (one row per forward event)
    forwards = relationship(
        'ReportsForward',
        order_by='ReportsForward.event_time',
        cascade='all,delete-orphan',
        passive_deletes=True,
        lazy='subquery',
    )

    # Relationship to normalized transfers table (one row per transfer event)
    transfers = relationship(
        'ReportsTransfer',
        order_by='ReportsTransfer.event_time',
        cascade='all,delete-orphan',
        passive_deletes=True,
        lazy='subquery',
    )

    @property
    def destination_details_dict(self):
        return {
            row.destination_details_key: row.destination_details_value
            for row in self.destination_details
        }

    destination_participant = relationship(
        'ReportsCallLogParticipant',
        primaryjoin='''and_(
            ReportsCallLogParticipant.call_log_id == ReportsCallLog.id,
            ReportsCallLogParticipant.role == 'destination'
        )''',
        order_by='desc(ReportsCallLogParticipant.answered), desc(ReportsCallLogParticipant.user_uuid)',
        viewonly=True,
        uselist=False,
    )
    destination_user_uuid = association_proxy('destination_participant', 'user_uuid')
    destination_line_id = association_proxy('destination_participant', 'line_id')

    cel_ids = []

    __table_args__ = (
        Index('plugin_reports_call_log__idx__conversation_id', 'conversation_id'),
        CheckConstraint(
            direction.in_(['inbound', 'internal', 'outbound']),
            name='plugin_reports_call_log_direction_check',
        ),
    )

    @hybrid_property
    def requested_user_uuid(self):
        for participant in self.participants:
            if participant.requested:
                return participant.user_uuid
        return None

    @requested_user_uuid.expression
    def requested_user_uuid(cls):
        return (
            select([ReportsCallLogParticipant.user_uuid])
            .where(
                and_(
                    ReportsCallLogParticipant.requested.is_(True),
                    ReportsCallLogParticipant.call_log_id == cls.id,
                )
            )
            .as_scalar()
        )


@generic_repr
class ReportsDestination(Base):
    __tablename__ = 'plugin_reports_call_log_destination'

    uuid = Column(
        UUIDType,
        server_default=text('uuid_generate_v4()'),
        primary_key=True,
    )

    call_log_id = Column(
        Integer,
        ForeignKey(
            'plugin_reports_call_log.id',
            name='plugin_reports_call_log_destination_call_log_id_fkey',
            ondelete='CASCADE',
        ),
    )

    destination_details_key = Column(String(32), nullable=False)
    destination_details_value = Column(String(255), nullable=False)

    __table_args__ = (
        Index('plugin_reports_call_log_destination__idx__uuid', 'uuid'),
        Index('plugin_reports_call_log_destination__idx__call_log_id', 'call_log_id'),
        CheckConstraint(
            destination_details_key.in_(
                [
                    'type',
                    'user_uuid',
                    'user_name',
                    'meeting_uuid',
                    'meeting_name',
                    'conference_id',
                    'group_label',
                    'group_id',
                ]
            ),
            name='plugin_reports_call_log_destination_details_key_check',
        ),
    )


@generic_repr
class ReportsCallLogParticipant(Base):
    __tablename__ = 'plugin_reports_call_log_participant'
    __table_args__ = (
        Index('plugin_reports_call_log_participant__idx__user_uuid', 'user_uuid'),
        Index('plugin_reports_call_log_participant__idx__call_log_id', 'call_log_id'),
    )

    uuid = Column(
        UUIDType,
        server_default=text('uuid_generate_v4()'),
        primary_key=True,
    )
    call_log_id = Column(
        Integer,
        ForeignKey(
            'plugin_reports_call_log.id',
            name='plugin_reports_call_log_participant_call_log_id_fkey',
            ondelete='CASCADE',
        ),
    )
    user_uuid = Column(UUIDType, nullable=False)
    line_id = Column(Integer)
    role = Column(
        Enum(
            'source',
            'destination',
            name='plugin_reports_call_log_participant_role',
        ),
        nullable=False,
    )
    tags = Column(
        MutableList.as_mutable(ARRAY(String(128))), nullable=False, server_default='{}'
    )
    answered = Column(Boolean, nullable=False, server_default='false')
    requested = Column(Boolean, nullable=False, server_default='false')

    call_log = relationship('ReportsCallLog', uselist=False, viewonly=True)

    @hybrid_property
    def peer_exten(self):
        if self.role == 'source':
            return self.call_log.requested_exten
        else:
            return self.call_log.source_exten

    @peer_exten.expression
    def peer_exten(cls):
        return case(
            [
                (
                    cls.role == 'source',
                    select([ReportsCallLog.requested_exten])
                    .where(cls.call_log_id == ReportsCallLog.id)
                    .as_scalar(),
                )
            ],
            else_=select([ReportsCallLog.source_exten])
            .where(cls.call_log_id == ReportsCallLog.id)
            .as_scalar(),
        )


@generic_repr
class ReportsRecording(Base):
    __tablename__ = 'plugin_reports_recording'
    __table_args__ = (Index('plugin_reports_recording__idx__call_log_id', 'call_log_id'),)

    uuid = Column(
        UUIDType(),
        server_default=text('uuid_generate_v4()'),
        primary_key=True,
    )
    start_time = Column(DateTime(timezone=True), nullable=False)
    end_time = Column(DateTime(timezone=True), nullable=False)
    path = Column(Text)
    call_log_id = Column(
        Integer(),
        ForeignKey(
            'plugin_reports_call_log.id',
            name='plugin_reports_recording_call_log_id_fkey',
            ondelete='CASCADE',
        ),
        nullable=False,
    )
    conversation_id = association_proxy('call_log', 'conversation_id')

    @property
    def filename(self):
        offset = self.start_time.utcoffset() or td(seconds=0)
        date_utc = (self.start_time - offset).replace(tzinfo=tz.utc)
        utc_start = date_utc.strftime('%Y-%m-%dT%H_%M_%SUTC')
        return '{start}-{cdr_id}-{uuid}.wav'.format(
            start=utc_start,
            cdr_id=self.call_log_id,
            uuid=self.uuid,
        )

    def __init__(self, mixmonitor_id=None, *args, **kwargs):
        # NOTE(fblackburn): Used to track recording on generation
        self.mixmonitor_id = mixmonitor_id
        super().__init__(*args, **kwargs)

    @property
    def deleted(self):
        return self.path is None

    call_log = relationship(ReportsCallLog, uselist=False, viewonly=True)


@generic_repr
class ReportsForward(Base):
    """Normalized table storing individual forward events for a call log.

    One row per forward action (e.g. user forward). Useful for querying/analytics.
    """

    __tablename__ = 'plugin_reports_call_log_forward'

    id = Column(Integer, nullable=False, primary_key=True)
    call_log_id = Column(
        Integer,
        ForeignKey(
            'plugin_reports_call_log.id',
            name='plugin_reports_call_log_forward_call_log_id_fkey',
            ondelete='CASCADE',
        ),
        nullable=False,
    )
    cel_id = Column(Integer)
    event_time = Column(DateTime(timezone=True))
    num = Column(String(64))
    context = Column(String(255))
    name = Column(String(255))
    channame = Column(String(255))
    created_at = Column(DateTime(timezone=True), server_default=text('now()'))

    call_log = relationship('ReportsCallLog', uselist=False)

    __table_args__ = (
        Index('plugin_reports_call_log_forward__idx__call_log_id', 'call_log_id'),
        Index('plugin_reports_call_log_forward__idx__event_time', 'event_time'),
    )


@generic_repr
class ReportsTransfer(Base):
    """Normalized table storing individual transfer events for a call log.

    One row per transfer action (blind or attended). Useful for querying/analytics.
    """

    __tablename__ = 'plugin_reports_call_log_transfer'

    id = Column(Integer, nullable=False, primary_key=True)
    call_log_id = Column(
        Integer,
        ForeignKey(
            'plugin_reports_call_log.id',
            name='plugin_reports_call_log_transfer_call_log_id_fkey',
            ondelete='CASCADE',
        ),
        nullable=False,
    )
    cel_id = Column(Integer)
    event_time = Column(DateTime(timezone=True))
    transfer_type = Column(String(32))
    target_exten = Column(String(64))
    context = Column(String(255))

    # channel information from extra payload
    transferee_channel_name = Column(String(255))
    transferee_channel_uniqueid = Column(String(255))
    channel2_name = Column(String(255))
    channel2_uniqueid = Column(String(255))
    transfer_target_channel_name = Column(String(255))
    transfer_target_channel_uniqueid = Column(String(255))

    # bridge identifiers
    bridge1_id = Column(String(64))
    bridge2_id = Column(String(64))

    # extracted short line names (e.g. 8gfq9ytw from PJSIP/8gfq9ytw-00000042)
    transferee_line = Column(String(128))
    transfer_target_line = Column(String(128))
    channel2_line = Column(String(128))

    created_at = Column(DateTime(timezone=True), server_default=text('now()'))

    call_log = relationship('ReportsCallLog', uselist=False)

    __table_args__ = (
        Index('plugin_reports_call_log_transfer__idx__call_log_id', 'call_log_id'),
        Index('plugin_reports_call_log_transfer__idx__event_time', 'event_time'),
    )

