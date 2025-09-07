import os
from marshmallow import fields, validates, ValidationError, validates_schema
from wazo_confd.helpers.mallow import BaseSchema
from xivo.mallow.validate import Length, OneOf, Range, Regexp

class ReportsRequestSchema(BaseSchema):
    # External query param names: 'from' and 'until' but keep internal keys start_time/end_time
    start_time = fields.String(data_key='from', allow_none=True)
    end_time = fields.String(data_key='until', allow_none=True)
    schedule_id = fields.Integer(allow_none=True)