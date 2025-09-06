import os
from marshmallow import fields, validates, ValidationError, validates_schema
from wazo_confd.helpers.mallow import BaseSchema
from xivo.mallow.validate import Length, OneOf, Range, Regexp

class ReportsRequestSchema(BaseSchema):
    # External query param names: 'from' and 'until' but keep internal keys start_time/end_time
    start_time = fields.String(data_key='from', allow_none=True)
    end_time = fields.String(data_key='until', allow_none=True)
    schedule_id = fields.Integer(allow_none=True)
    work_start = fields.String(allow_none=True, validate=Regexp(r'^\d{2}:\d{2}$'))
    work_end = fields.String(allow_none=True, validate=Regexp(r'^\d{2}:\d{2}$'))

    @validates_schema
    def validate_time_range(self, data, **kwargs):
        # Ensure if one of work_start/work_end is provided both are present
        if ('work_start' in data and data.get('work_start') is not None) ^ ('work_end' in data and data.get('work_end') is not None):
            raise ValidationError('Both work_start and work_end must be provided together', field_name='work_start')