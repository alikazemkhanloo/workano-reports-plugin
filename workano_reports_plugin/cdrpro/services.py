from wazo_confd.helpers.resource import CRUDService

from . import dao
from .notifier import build_survey_notifier, build_queuefeature_notifier
from .validator import build_survey_validator, build_queuefeature_validator


class CDRService():
    def __init__(self, dao, validator, notifier, extra_parameters=None):
        self.dao = dao
        self.validator = validator
        self.notifier = notifier
        self.extra_parameters = extra_parameters or []

    def get_cdr(self, tenant_uuid, params):
        return dao.get_cdr(tenant_uuid, params)


def build_cdr_pro_service():
    return CDRService(dao, build_survey_validator(), build_survey_notifier())

