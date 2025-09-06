from xivo_dao.helpers.persistor import BasePersistor
from xivo_dao.resources.utils.search import CriteriaBuilderMixin

from .model import OtpModel


class OtpPersistor(CriteriaBuilderMixin, BasePersistor):
    _search_table = OtpModel

    def __init__(self, session, otp_request_search, tenant_uuids=None):
        self.session = session
        self.search_system = otp_request_search
        self.tenant_uuids = tenant_uuids

    def _find_query(self, criteria):
        query = self.session.query(OtpModel)
        # query = self._filter_tenant_uuid(query)
        return self.build_criteria(query, criteria)

    def _search_query(self):
        return self.session.query(self.search_system.config.table)

    def create(self, model):
        self.session.add(model)
        self.session.commit()  # commit added here
        return model

    def edit(self, model):
        self.persist(model)
        self.session.commit()