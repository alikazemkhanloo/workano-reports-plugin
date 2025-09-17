from typing import Any
from xivo_dao.helpers.persistor import BasePersistor
from xivo_dao.resources.utils.search import CriteriaBuilderMixin
from datetime import date, datetime, timedelta
from sqlalchemy.types import DateTime
from ..cel_interpretor.models import ReportsCallLog as CallLog, ReportsCallLogParticipant as CallLogParticipant
from wazo_confd_survey.survey.model import SurveyModel
from wazo_confd_call_note.call_info.model import CallInfoModel
from wazo_confd_call_note.tag.model import TagModel
import sqlalchemy as sa
from sqlalchemy import and_, distinct, func, sql, cast, or_, case, Integer
from sqlalchemy.orm import Query, joinedload, selectinload, subqueryload
from sqlalchemy.dialects.postgresql import ARRAY, UUID
from xivo_dao.alchemy.userfeatures import UserFeatures as User

class SurveyPersistor(CriteriaBuilderMixin, BasePersistor):
    _search_table = SurveyModel

    searched_columns = (
        CallLog.source_name,
        CallLog.source_exten,
        CallLog.destination_name,
        CallLog.destination_exten,
    )
    def __init__(self, session, survey_search, tenant_uuids=None):
        self.session = session
        self.search_system = survey_search
        self.tenant_uuids = tenant_uuids

    def _apply_filters(self, query: Query, params: dict[str, Any]) -> Query:
        if start_date := params.get('start'):
            query = query.filter(CallLog.date >= start_date)
        if end_date := params.get('end'):
            query = query.filter(CallLog.date < end_date)

        if call_direction := params.get('call_direction'):
            query = query.filter(CallLog.direction == call_direction)

        if cdr_ids := params.get('cdr_ids'):
            query = query.filter(CallLog.id.in_(cdr_ids))

        if call_log_id := params.get('id'):
            query = query.filter(CallLog.id == call_log_id)

        if search := params.get('search'):
            filters = (
                sql.cast(column, sa.String).ilike(f'%{search}%')
                for column in self.searched_columns
            )
            query = query.filter(sql.or_(*filters))

        if number := params.get('number'):
            filters = (
                sql.cast(column, sa.String).like(f"{number.replace('_', '%')}")
                for column in [
                    CallLog.source_exten,
                    CallLog.destination_exten,
                ]
            )
            query = query.filter(sql.or_(*filters))

        # for tag in params.get('tags', []):
        #     query = query.filter(
        #         CallLog.participants.any(
        #             CallLogParticipant.tags.contains(sql.cast([tag], ARRAY(sa.String)))
        #         )
        #     )
        for tag in params.get('tags', []):
            query = query.filter(
                 or_(
                    CallInfoModel.tag_ids == tag,
                    CallInfoModel.tag_ids.like(f"{tag},%"),
                    CallInfoModel.tag_ids.like(f"%,{tag},%"),
                    CallInfoModel.tag_ids.like(f"%,{tag}")
                )
            )

        if tenant_uuids := params.get('tenant_uuids'):
            query = query.filter(
                CallLog.tenant_uuid.in_(str(uuid) for uuid in tenant_uuids)
            )

        if me_user_uuid := params.get('me_user_uuid'):
            query = query.filter(
                CallLog.participants.any(
                    CallLogParticipant.user_uuid == str(me_user_uuid)
                )
            )

        if user_uuids := params.get('user_uuids'):
            filters = (
                CallLog.participants.any(CallLogParticipant.user_uuid == str(user_uuid))
                for user_uuid in user_uuids
            )
            query = query.filter(sql.or_(*filters))

        if terminal_user_uuids := params.get('terminal_user_uuids'):
            # consider only source participant
            # and destination participant(first 'destination' participant to have answered)
            # NOTE(clanglois): if no participant has answered, destination participant is
            # an arbitrary 'destination' participant based on uuid ordering
            # NOTE(clanglois): destination participant definition must be reimplemented here
            # in order to be used for filtering because of limitation of relationship
            filters = sql.or_(
                (
                    CallLog.source_participant.has(
                        CallLogParticipant.user_uuid.in_(
                            str(terminal_user_uuid)
                            for terminal_user_uuid in terminal_user_uuids
                        )
                    )
                ),
                (
                    Query(CallLogParticipant.user_uuid)
                    .filter(
                        CallLogParticipant.role == 'destination',
                        CallLogParticipant.call_log_id == CallLog.id,
                    )
                    .order_by(
                        sql.desc(CallLogParticipant.answered),
                        sql.desc(CallLogParticipant.user_uuid),
                    )
                    .limit(1)
                    .subquery()
                    == sql.any_(
                        sql.cast(
                            [
                                str(terminal_user_uuid)
                                for terminal_user_uuid in terminal_user_uuids
                            ],
                            ARRAY(UUID),
                        )
                    )
                ),
            )
            query = query.filter(filters)

        if start_id := params.get('start_id'):
            query = query.filter(CallLog.id >= start_id)

        if (recorded := params.get('recorded')) is not None:
            if recorded:
                query = query.filter(CallLog.recordings.any())
            else:
                query = query.filter(~CallLog.recordings.any())

        if conversation_id := params.get('conversation_id'):
            query = query.filter(CallLog.conversation_id == conversation_id)

        answered = params.get('answered')
        if (answered is not None):
            if(answered == True):
                query = query.filter(CallLog.date_answer.isnot(None))
            elif (answered == False):
                query = query.filter(CallLog.date_answer == None)

        has_voicemail = params.get('has_voicemail')
        if (has_voicemail is not None):
            if(has_voicemail == True):
                query = query.filter(SurveyModel.voicemail_id.isnot(None))
            elif (has_voicemail == False):
                query = query.filter(SurveyModel.voicemail_id == None)

        has_forward = params.get('has_forward')
        if (has_forward is not None):
            if has_forward == True:
                query = query.filter(CallLog.forwards.any())
            else:
                query = query.filter(~CallLog.forwards.any())

        has_transfer = params.get('has_transfer')
        if (has_transfer is not None):
            if has_transfer == True:
                query = query.filter(CallLog.transfers.any())
            else:
                query = query.filter(~CallLog.transfers.any())

        if rating := params.get('rating'):
            query = query.filter(SurveyModel.rate == str(rating))

        return query
    
    def get_cdr(self, tenant_uuid, params):
        query = (
            self.session.query(CallLog, SurveyModel, CallInfoModel)
            .filter(CallLog.tenant_uuid == tenant_uuid)
            .outerjoin(SurveyModel, SurveyModel.linked_id == CallLog.conversation_id)
            .outerjoin(CallInfoModel, CallInfoModel.cdr_id == CallLog.id)
        )
        distinct_ = params.get('distinct')
        if distinct_ == 'peer_exten':
            # TODO(pcm) use the most recent call log not the most recent id
            sub_query = (
                self.session.query(func.max(CallLogParticipant.call_log_id).label('max_id'))
                .group_by(CallLogParticipant.user_uuid, CallLogParticipant.peer_exten)
                .subquery()
            )

            query = query.join(
                sub_query, and_(CallLog.id == sub_query.c.max_id)
            )
        else:
            pass # query = query

        query = query.options(
            joinedload('participants'),
            joinedload('recordings'),
            selectinload('recordings.call_log'),
            subqueryload('source_participant'),
            subqueryload('destination_participant'),
            selectinload(CallLog.forwards),
            selectinload(CallLog.transfers),
        )

        count = query.count()
        query = self._apply_filters(query, params)


        # add order
        order_field = None
        if params.get('order'):
            if params['order'] == 'marshmallow_duration':
                order_field = CallLog.date_end - CallLog.date_answer
            elif params['order'] == 'marshmallow_answered':
                order_field = CallLog.date_answer
            else:
                order_field = getattr(CallLog, params['order'])
        if params.get('direction') == 'desc':
            order_field = order_field.desc().nullslast()
        if params.get('direction') == 'asc':
            order_field = order_field.asc().nullsfirst()

        if order_field is not None:
            query = query.order_by(order_field)

        filtered = query.count()
        tag_array = case(
            [(
                or_(CallInfoModel.tag_ids.is_(None), CallInfoModel.tag_ids == ''),
                func.string_to_array('NO_TAGS', ',')  # array literal
            )],
            else_=func.string_to_array(CallInfoModel.tag_ids, ',')
        )
        tag_id = func.unnest(tag_array).label('tag_id')
        answered = case(
                [(CallLog.date_answer.isnot(None), True)],
                else_=False
            ).label('answered')
        count_query = query.join(CallLog.participants)  # Needed for GROUP BY, FILTERS
        count_query = count_query.outerjoin(User, cast(User.uuid, UUID) == CallLogParticipant.user_uuid)
        duration = func.sum(
            case(
                [
                    (CallLog.date_answer.isnot(None), (CallLog.date_end - CallLog.date_answer))
                ],
                else_=None
            )
        ).label('duration')

        count_query =  (
            count_query.with_entities(
                func.count().label('count'),
                duration,
                # func.unnest(func.string_to_array(func.coalesce(CallInfoModel.tag_ids, ''), ',')).label('tag_id'),
                CallLogParticipant.user_uuid,
                func.max(User.firstname).label('firstname'),
                func.max(User.lastname).label('lastname'),
            )
            .order_by(None)
        )
        count_query = count_query.group_by(
            CallLogParticipant.user_uuid
        )
        count_query_with_tags =(
            count_query
            .add_columns(
                tag_id,
                CallLog.direction,
                answered,
            )
            .group_by(
                CallLog.direction,
                answered,
                CallLogParticipant.user_uuid,
                tag_id,
            )
        )
        if params.get('format') != 'csv':
            if params.get('limit'):
                query = query.limit(params['limit'])
            if params.get('offset'):
                query = query.offset(params['offset'])

        tags = self.session.query(TagModel)
        return (query, count, filtered, count_query, count_query_with_tags, tags)

    def _find_query(self, criteria):
        query = self.session.query(SurveyModel)
        return self.build_criteria(query, criteria)

    def _search_query(self):
        return self.session.query(self.search_system.config.table)

    def get_all_surveys(self, tenant_uuid, queue_id):
        query = self.session.query(SurveyModel)
        query = query.filter(SurveyModel.tenant_uuid == tenant_uuid)
        query = query.filter(SurveyModel.queue_id == queue_id)
        return query

    def get_all_survey_by_queue_id(self, tenant_uuid, queue_id):
        query = self.session.query(SurveyModel)
        query = query.filter(SurveyModel.tenant_uuid == tenant_uuid)
        query = query.filter(SurveyModel.queue_id == queue_id)
        return query

    def get_all_survey_by_agent_id(self, tenant_uuid, agent_id):
        query = self.session.query(SurveyModel)
        query = query.filter(SurveyModel.tenant_uuid == tenant_uuid)
        query = query.filter(SurveyModel.agent_id == agent_id)
        return query

    def get_average_survey_by_queue_id(self, tenant_uuid, queue_id, from_date, until_date):
        from_date = datetime.fromisoformat(from_date) if isinstance(from_date, str) else from_date
        until_date = datetime.fromisoformat(until_date) if isinstance(until_date, str) else until_date
        until_date = until_date + timedelta(days=1)  # Include the entire day for until_date

        query = self.session.query(SurveyModel)
        query = query.filter(SurveyModel.tenant_uuid == tenant_uuid)
        query = query.filter(SurveyModel.queue_id == queue_id)
        query = query.filter(cast(SurveyModel.timestamp, DateTime) >= from_date)
        query = query.filter(cast(SurveyModel.timestamp, DateTime) < until_date)
        return query

    def get_average_survey_by_agent_id(self, tenant_uuid, agent_id, from_date, until_date):
        from_date = datetime.fromisoformat(from_date) if isinstance(from_date, str) else from_date
        until_date = datetime.fromisoformat(until_date) if isinstance(until_date, str) else until_date
        until_date = until_date + timedelta(days=1)  # Include the entire day for until_date

        query = self.session.query(SurveyModel)
        query = query.filter(SurveyModel.tenant_uuid == tenant_uuid)
        query = query.filter(SurveyModel.agent_id == agent_id)
        query = query.filter(cast(SurveyModel.timestamp, DateTime) >= from_date)
        query = query.filter(cast(SurveyModel.timestamp, DateTime) < until_date)
        return query

    def get_average_survey_all_agent(self, tenant_uuid, from_date, until_date):
        from_date = datetime.fromisoformat(from_date) if isinstance(from_date, str) else from_date
        until_date = datetime.fromisoformat(until_date) if isinstance(until_date, str) else until_date
        until_date = until_date + timedelta(days=1)  # Include the entire day for until_date

        query = self.session.query(SurveyModel)
        query = query.filter(SurveyModel.tenant_uuid == tenant_uuid)
        query = query.filter(cast(SurveyModel.timestamp, DateTime) >= from_date)
        query = query.filter(cast(SurveyModel.timestamp, DateTime) < until_date)
        return query

    def get_average_survey_all_queue(self, tenant_uuid, from_date, until_date):
        from_date = datetime.fromisoformat(from_date) if isinstance(from_date, str) else from_date
        until_date = datetime.fromisoformat(until_date) if isinstance(until_date, str) else until_date
        until_date = until_date + timedelta(days=1)  # Include the entire day for until_date

        query = self.session.query(SurveyModel)
        query = query.filter(SurveyModel.tenant_uuid == tenant_uuid)
        query = query.filter(cast(SurveyModel.timestamp, DateTime) >= from_date)
        query = query.filter(cast(SurveyModel.timestamp, DateTime) < until_date)
        return query

    def get_average_survey_agent_queue(self, tenant_uuid, queue_id, agent_id, from_date, until_date):
        from_date = datetime.fromisoformat(from_date) if isinstance(from_date, str) else from_date
        until_date = datetime.fromisoformat(until_date) if isinstance(until_date, str) else until_date
        until_date = until_date + timedelta(days=1)  # Include the entire day for until_date

        query = self.session.query(SurveyModel)
        query = query.filter(SurveyModel.tenant_uuid == tenant_uuid)
        query = query.filter(SurveyModel.agent_id == agent_id)
        query = query.filter(SurveyModel.queue_id == queue_id)
        query = query.filter(cast(SurveyModel.timestamp, DateTime) >= from_date)
        query = query.filter(cast(SurveyModel.timestamp, DateTime) < until_date)
        return query

    def get_average_survey_all_agents_in_queue(self, tenant_uuid, queue_id, from_date, until_date):
        from_date = datetime.fromisoformat(from_date) if isinstance(from_date, str) else from_date
        until_date = datetime.fromisoformat(until_date) if isinstance(until_date, str) else until_date
        until_date = until_date + timedelta(days=1)  # Include the entire day for until_date

        query = self.session.query(SurveyModel)
        query = query.filter(SurveyModel.tenant_uuid == tenant_uuid)
        query = query.filter(SurveyModel.queue_id == queue_id)
        query = query.filter(cast(SurveyModel.timestamp, DateTime) >= from_date)
        query = query.filter(cast(SurveyModel.timestamp, DateTime) < until_date)
        return query

