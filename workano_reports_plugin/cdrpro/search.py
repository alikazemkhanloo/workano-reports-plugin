from xivo_dao.resources.utils.search import SearchConfig
from xivo_dao.resources.utils.search import SearchSystem

from .model import  QueueFeaturesModel
from wazo_confd_survey.survey.model import SurveyModel


survey_config = SearchConfig(
    table=SurveyModel,
    columns={
        'id': SurveyModel.id,
        'tenant_uuid': SurveyModel.tenant_uuid,
        'queue_id': SurveyModel.queue_id,
    },
    default_sort='id',
)

queuefeature_config = SearchConfig(
    table=QueueFeaturesModel,
    columns={
        'id': QueueFeaturesModel.id,
        'queue_id': QueueFeaturesModel.queue_id,
    },
    default_sort='id',
)

survey_search = SearchSystem(survey_config)

queuefeature_search = SearchSystem(queuefeature_config)
