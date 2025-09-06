import logging
from functools import wraps

from marshmallow import ValidationError

# from ari.exceptions import ARIException, ARIHTTPError
from .services import OtpPlaybackService
from xivo import mallow_helpers, rest_api_helpers
from xivo.flask.auth_verifier import AuthVerifierFlask
# from .exceptions import AsteriskARIError, AsteriskARIUnreachable


from flask import url_for, request
from wazo_confd.auth import required_acl
# from wazo_calld.http import  Resource
from flask_restful import Resource

from .model import OtpModel
from .schema import OtpReportSchema, OtpRequestSchema, OtpUploadRequestSchema, ReportItemRequestSchema, ReportRequestSchema

auth_verifier = AuthVerifierFlask()
logger = logging.getLogger(__name__)

def handle_ari_exception(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            raise e
        # except ARIHTTPError as e:
        #     raise AsteriskARIError(
        #         {'base_url': e.client.base_url}, e.original_error, e.original_message
        #     )
        # except ARIException as e:
        #     raise AsteriskARIUnreachable(
        #         {'base_url': e.client.base_url}, e.original_error, e.original_message
        #     )

    return wrapper


class ErrorCatchingResource(Resource):
    method_decorators = [
        mallow_helpers.handle_validation_exception,
        handle_ari_exception,
        rest_api_helpers.handle_api_exception,
    ] + Resource.method_decorators

class OtpPlaybackResource(Resource):
    def __init__(self, service):
        super().__init__()
        self.service: OtpPlaybackService = service

    schema = OtpReportSchema
    model = OtpModel
    request_schema = OtpRequestSchema

    def build_headers(self, model):
        return {'Location': url_for('create_otp', uuid=model.application_uuid, _external=True)}

    @required_acl('workano.otp.request')
    def post(self):
        if request.is_json:
            data = request.get_json()
        else:
            json_part = request.form.to_dict()
            file_part = request.files.get('file')
            data = {**json_part, 'file': file_part}

        try:
            form = self.request_schema().load(data)
        except ValidationError as err:
            return {'errors': err.messages}, 400

        model = self.service.process_otp_request(form)
        if model['result']:
            result = self.schema().dump(model['result'])
            return {'id': result['id']}, 200, self.build_headers(model['result'])
        else:
            return model['error'], 400

class OtpFileUploadResource(Resource):
    def __init__(self, service):
        super().__init__()
        self.service: OtpPlaybackService = service
    
    @required_acl('workano.otp.upload')
    def post(self):
        json_part = request.form.to_dict()
        file_part = request.files.get('file')
        data = {**json_part, 'file': file_part}
        schema = OtpUploadRequestSchema()
        try:
            result = schema.load(data)
        except ValidationError as err:
            return {'errors': err.messages}, 400
        self.service.process_upload(result)
        return {'message': 'File uploaded successfully'}

class OtpReportResource(Resource):
    def __init__(self, service):
        super().__init__()
        self.service: OtpPlaybackService = service
    
    report_request_schema = ReportRequestSchema
    schema = OtpReportSchema

    @required_acl('workano.otp.report')
    def get(self):
        form = self.report_request_schema().load(request.args)
        report = self.service.get_report(form)
        if report['result']:
            return self.schema().dump(report['result'], many=True)
        else:
            return report['error'], 400



class OtpReportItemResource(Resource):
    def __init__(self, service):
        super().__init__()
        self.service: OtpPlaybackService = service
    
    report_item_request_schema = ReportItemRequestSchema
    schema = OtpReportSchema

    @required_acl('workano.otp.report')
    def get(self, id):
        form = self.report_item_request_schema().load(request.args)
        report = self.service.get_report(form, id)
        if report['result']:
            return self.schema().dump(report['result'])
        else:
            return report['error'], 400
