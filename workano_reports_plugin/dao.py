from workano_otp_plugin.model import OtpModel
from .db import ScopedSession
from xivo_dao.helpers.db_manager import daosession

from .persistor import OtpPersistor
from .search import otp_request_search


@daosession
def _persistor(session, tenant_uuids=None):
    s = ScopedSession
    return OtpPersistor(s, otp_request_search, tenant_uuids)


def search(tenant_uuids=None, **parameters):
    return _persistor(tenant_uuids).search(parameters)


def get(otp_uuid, tenant_uuids=None):
    return _persistor(tenant_uuids).get_by({'uuid': otp_uuid})


def get_by(tenant_uuids=None, **criteria) -> OtpModel:
    return _persistor(tenant_uuids).get_by(criteria)


def find(otp_uuid, tenant_uuids=None):
    return _persistor(tenant_uuids).find_by({'uuid': otp_uuid})


def find_by(tenant_uuids=None, **criteria):
    return _persistor(tenant_uuids).find_by(criteria)


def find_all_by(tenant_uuids=None, **criteria):
    return _persistor(tenant_uuids).find_all_by(criteria)


def create(otp_request):
    return _persistor().create(otp_request)


def edit(otp_request):
    _persistor().edit(otp_request)


def delete(otp_request):
    _persistor().delete(otp_request)
