from xivo_dao.resources.utils.search import SearchConfig
from xivo_dao.resources.utils.search import SearchSystem

from .model import OtpModel

otp_request_config = SearchConfig(
    table=OtpModel,
    columns={
        'uuid': OtpModel.uuid,
        'call_id': OtpModel.call_id,
        'number': OtpModel.number,
        'caller_id_name': OtpModel.caller_id_name,
        'caller_id_number': OtpModel.caller_id_number,
    },
    default_sort='caller_id_name',
)

otp_request_search = SearchSystem(otp_request_config)
