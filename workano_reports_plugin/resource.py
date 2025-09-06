import logging
from functools import wraps


# from ari.exceptions import ARIException, ARIHTTPError
from .services import WorkanoReportsService
from xivo import mallow_helpers, rest_api_helpers
from xivo.flask.auth_verifier import AuthVerifierFlask


from flask import url_for, request
from wazo_confd.auth import required_acl
# from wazo_calld.http import  Resource
from flask_restful import Resource


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

class ReportsResource(AuthResource):
    def __init__(self, service):
        super().__init__()
        self.service: WorkanoReportsService = service

    @required_acl('workano.otp.request')
    def get(self):
        
        return {}, 400
