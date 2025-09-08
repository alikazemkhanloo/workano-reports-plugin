# Copyright 2013-2023 The Wazo Authors  (see the AUTHORS file)
# SPDX-License-Identifier: GPL-3.0-or-later
from wazo_call_logd.database.queries import DAO
from xivo_dao.helpers.db_manager import daosession

from workano_reports_plugin.models import ReportsCallLog

@daosession
def delete_from_list(session, call_log_ids):
    query = session.query(ReportsCallLog)
    query = query.filter(ReportsCallLog.id.in_(call_log_ids))
    query.delete(synchronize_session=False)


@daosession
def create_from_list(session, call_logs):
    if not call_logs:
        return
    for call_log in call_logs:
        session.add(call_log)
        session.flush()
        # NOTE(fblackburn): fetch relationship before expunge_all
        call_log.recordings
        call_log.source_participant
        call_log.destination_participant
    session.expunge_all()


class CallLogsWriter:
    def __init__(self, dao):
        self._dao: DAO = dao

    def write(self, call_logs):
        delete_from_list(call_logs.call_logs_to_delete)
        # self._dao.cel.unassociate_all_from_call_log_ids(call_logs.call_logs_to_delete)
        tenant_uuids = {cdr.tenant_uuid for cdr in call_logs.new_call_logs}
        self._dao.tenant.create_all_uuids_if_not_exist(tenant_uuids)
        create_from_list(call_logs.new_call_logs)
        # self._dao.cel.associate_all_to_call_logs(call_logs.new_call_logs)