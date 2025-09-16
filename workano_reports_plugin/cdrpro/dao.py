from xivo_dao.helpers.db_manager import daosession
from .persistor import SurveyPersistor
from .search import survey_search

from datetime import timedelta


@daosession
def _persistor(session, tenant_uuids=None):
    return SurveyPersistor(session, survey_search, tenant_uuids)


def get_cdr(tenant_uuid, params):
    return _persistor().get_cdr(tenant_uuid, params)



def get_by(tenant_uuids=None, **criteria):
    return _persistor(tenant_uuids).get_by(criteria)

def get_all_surveys(tenant_uuid, queue_id):
    return _persistor().get_all_surveys(tenant_uuid, queue_id)


def get_all_survey_by_queue_id(tenant_uuid, queue_id):
    return _persistor().get_all_survey_by_queue_id(tenant_uuid, queue_id)


def get_all_survey_by_agent_id(tenant_uuid, agent_id):
    return _persistor().get_all_survey_by_agent_id(tenant_uuid, agent_id)


def get_average_survey_by_queue_id(tenant_uuid, queue_id, from_date, until_date):
    return _persistor().get_average_survey_by_queue_id(tenant_uuid, queue_id, from_date, until_date)


def get_average_survey_by_agent_id(tenant_uuid, agent_id, from_date, until_date):
    return _persistor().get_average_survey_by_agent_id(tenant_uuid, agent_id, from_date, until_date)


def get_average_survey_all_agent(tenant_uuid, from_date, until_date):
    return _persistor().get_average_survey_all_agent(tenant_uuid, from_date, until_date)


def get_average_survey_all_queue(tenant_uuid, from_date, until_date):
    return _persistor().get_average_survey_all_queue(tenant_uuid, from_date, until_date)


def get_average_survey_agent_queue(tenant_uuid, queue_id, agent_id, from_date, until_date):
    return _persistor().get_average_survey_agent_queue(tenant_uuid, queue_id, agent_id, from_date, until_date)


def get_average_survey_all_agents_in_queue(tenant_uuid, queue_id, from_date, until_date):
    return _persistor().get_average_survey_all_agents_in_queue(tenant_uuid, queue_id, from_date, until_date)
