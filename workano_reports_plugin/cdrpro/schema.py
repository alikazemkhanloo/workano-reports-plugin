from dataclasses import field
from wsgiref import validate
from marshmallow import Schema, fields, post_dump, post_load, pre_dump, pre_load
from wazo_confd.helpers.mallow import BaseSchema
from wazo_call_logd.plugins.cdr.schemas import DestinationDetailsField, BaseDestinationDetailsSchema
from xivo.mallow.validate import Length, OneOf, Range, Regexp
from wazo_confd_call_note.tag.schema import TagSchema
from workano_reports_plugin.cel_interpretor.models import LineFeatures

NUMBER_REGEX = r'^_?[0-9]+_?$'
CONVERSATION_ID_REGEX = r'^[0-9]+\.[0-9]+$'

class QueueFeaturesSchema(BaseSchema):
    id = fields.Integer(dump_only=True)
    tenant_uuid = fields.String(dump_only=True)
    queue_id = fields.String(dump_only=False)
    play_agentnumber_enable = fields.String(dump_only=False)
    queue_survey_enable = fields.String(dump_only=False)




class LineSchema(BaseSchema):
    id = fields.Integer(dump_only=True)
    name = fields.String(dump_only=True)
    number = fields.String(dump_only=True)

    

class ForwardSchema(BaseSchema):
    id = fields.Integer()
    cel_id = fields.Integer()
    event_time = fields.DateTime()
    num = fields.String()
    context = fields.String()
    name = fields.String()
    channame = fields.String()
    created_at = fields.DateTime()



class TransferSchema(BaseSchema):
    id = fields.Integer()
    cel_id = fields.Integer()
    event_time = fields.DateTime()
    transfer_type = fields.String()
    target_exten = fields.String()
    context = fields.String()
    transferee_channel_name = fields.String()
    transferee_channel_uniqueid = fields.String()
    channel2_name = fields.String()
    channel2_uniqueid = fields.String()
    transfer_target_channel_name = fields.String()
    transfer_target_channel_uniqueid = fields.String()
    bridge1_id = fields.String()
    bridge2_id = fields.String()
    transferee_line = fields.String()
    transfer_target_line = fields.String()
    channel2_line = fields.String()
    created_at = fields.DateTime()
    transfer_target_line_feature= fields.Nested(
        LineSchema, many=True, include=['name', 'number']
    )
    channel2_line_feature= fields.Nested(
        LineSchema, many=True, include=['name', 'number']
    )
class CDRSchema(BaseSchema):
    id = fields.Integer()
    tenant_uuid = fields.UUID()
    start = fields.DateTime(attribute='date')
    end = fields.DateTime(attribute='date_end')
    answered = fields.Boolean(attribute='marshmallow_answered')
    answer = fields.DateTime(attribute='date_answer')
    duration = fields.TimeDelta(dump_default=None, attribute='marshmallow_duration')
    call_direction = fields.String(attribute='direction')
    conversation_id = fields.String()
    destination_details = DestinationDetailsField(
        BaseDestinationDetailsSchema,
        attribute='destination_details_dict',
        required=True,
    )
    destination_extension = fields.String(attribute='destination_exten')
    destination_internal_context = fields.String()
    destination_internal_extension = fields.String(
        attribute='destination_internal_exten'
    )
    destination_line_id = fields.Integer()
    destination_name = fields.String()
    destination_user_uuid = fields.UUID()
    requested_name = fields.String()
    requested_context = fields.String()
    requested_extension = fields.String(attribute='requested_exten')
    requested_internal_context = fields.String()
    requested_internal_extension = fields.String(attribute='requested_internal_exten')
    requested_user_uuid = fields.UUID()
    source_extension = fields.String(attribute='source_exten')
    source_internal_context = fields.String()
    source_internal_name = fields.String()
    source_internal_extension = fields.String(attribute='source_internal_exten')
    source_line_id = fields.Integer()
    source_name = fields.String()
    source_user_uuid = fields.UUID()
    tags = fields.List(fields.String(), attribute='marshmallow_tags')
    recordings = fields.Nested(
        'RecordingSchema', many=True, dump_default=[], exclude=('conversation_id',)
    )
    blocked = fields.Boolean()
    trunk = fields.String()
    user_field = fields.String()
    schedule_state = fields.Dict()
    original_call_log_id = fields.Integer()
    ivr_choices = fields.List(fields.Dict(), dump_default=[])
    source_line_identity = fields.String()
    destination_line_identity = fields.String()
    transfers = fields.Nested(TransferSchema, many=True, dump_default=[])
    forwards = fields.Nested(ForwardSchema, many=True, dump_default=[])


    @pre_dump
    def _compute_fields(self, data, **kwargs):
        data.marshmallow_answered = True if data.date_answer else False
        if data.date_answer and data.date_end:
            data.marshmallow_duration = data.date_end - data.date_answer
        return data

    @post_dump
    def fix_negative_duration(self, data, **kwargs):
        if data['duration'] is not None:
            data['duration'] = max(data['duration'], 0)
        return data

    @pre_dump
    def _populate_tags_field(self, data, **kwargs):
        data.marshmallow_tags = set()
        for participant in data.participants:
            data.marshmallow_tags.update(participant.tags)
        return data



class CDRListingBase(Schema):
    from_ = fields.DateTime(data_key='from', attribute='start', load_default=None)
    until = fields.DateTime(attribute='end', load_default=None)
    search = fields.String(load_default=None)
    call_direction = fields.String(
        validate=OneOf(['internal', 'inbound', 'outbound']), load_default=None
    )
    number = fields.String(validate=Regexp(NUMBER_REGEX), load_default=None)
    tags = fields.List(fields.String(), load_default=[])
    user_uuid = fields.List(fields.String(), load_default=[], attribute='user_uuids')
    from_id = fields.Integer(
        validate=Range(min=0), attribute='start_id', load_default=None
    )
    recurse = fields.Boolean(load_default=False)

    @pre_load
    def convert_tags_and_user_uuid_to_list(self, data, **kwargs):
        result = data.to_dict()
        if data.get('tags'):
            result['tags'] = data['tags'].split(',')
        if data.get('user_uuid'):
            result['user_uuid'] = data['user_uuid'].split(',')
        return result



class CDRListRequestSchema(CDRListingBase):
    direction = fields.String(validate=OneOf(['asc', 'desc']), load_default='desc')
    order = fields.String(
        validate=OneOf(set(CDRSchema().fields) - {'end', 'tags', 'recordings'}),
        load_default='start',
    )
    limit = fields.Integer(validate=Range(min=0), load_default=1000)
    offset = fields.Integer(validate=Range(min=0), load_default=None)
    distinct = fields.String(validate=OneOf(['peer_exten']), load_default=None)
    recorded = fields.Boolean(load_default=None)
    format = fields.String(validate=OneOf(['csv', 'json']), load_default=None)
    language = fields.String(validate=OneOf(['fa','en']), load_default='fa')
    conversation_id = fields.String(
        validate=Regexp(
            CONVERSATION_ID_REGEX, error='not a valid conversation identifier'
        ),
        load_default=None,
    )

    answered = fields.Boolean(load_default=None)
    rating = fields.Integer(validate=Range(min=1, max=5), load_default=None)
    has_voicemail = fields.Boolean(load_default=None)

    @post_load
    def map_order_field(self, in_data, **kwargs):
        try:
            mapped_order = CDRSchema().fields[in_data['order']].attribute
            if mapped_order:
                in_data['order'] = mapped_order
            return in_data
        except KeyError:
            return in_data


class SummarySchema(Schema):
    answered = fields.Boolean(load_default=None)
    direction = fields.String(load_default= None)
    count = fields.Integer(load_default=None)
    tag_id = fields.String(load_default=None)
    user_uuid = fields.String(load_default=None)
    firstname = fields.String(load_default=None)
    lastname = fields.String(load_default=None)
    tag = fields.Nested(TagSchema)
    duration = fields.TimeDelta(dump_default=None)

