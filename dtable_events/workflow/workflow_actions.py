import logging

from dtable_events.automations.refactor_actions import BaseContext, NotifyAction, SendEmailAction, \
    SendWechatAction, SendDingtalkAction, UpdateAction, AddRowAction, LockRecordAction, LinkRecordsAction, \
    RunPythonScriptAction
from dtable_events.db import init_db_session_class

logger = logging.getLogger(__name__)


def do_actions(dtable_uuid, workflow_config, node, row_id, config):
    db_session = init_db_session_class(config)()
    table_id = workflow_config.get('table_id')
    context = BaseContext(dtable_uuid, table_id, db_session)
    actions = node.get('before_actions', [])2
    # TODO: workflow do actions
    for action_info in actions:
        logger.info('start action: %s', action_info.get('type'))
        try:
            if action_info.get('type') == 'notify':
                users = action_info.get('users', [])
                users_column_key = action_info.get('users_column_key')
                msg = action_info.get('default_msg')
                NotifyAction(
                    context,
                    users,
                    msg,
                    NotifyAction.NOTIFY_TYPE_AUTOMATION_RULE,
                    row=self.row,
                    converted_row=self.converted_row,
                    users_column_key=users_column_key,
                    condition=self.trigger.get('condition'),
                    rule_id=self.rule_id,
                    rule_name=self.rule_name
                ).do_action()
            elif action_info.get('type') == 'send_email':
                msg = action_info.get('default_msg')
                subject = action_info.get('subject')
                send_to = action_info.get('send_to')
                copy_to = action_info.get('copy_to')
                account_id = int(action_info.get('account_id'))
                SendEmailAction(
                    context,
                    account_id,
                    subject,
                    msg,
                    send_to,
                    copy_to,
                    'automation-rules',
                    row=self.row,
                    converted_row=self.converted_row
                ).do_action()
            elif action_info.get('type') == 'send_wechat':
                account_id = int(action_info.get('account_id'))
                msg = action_info.get('default_msg')
                msg_type = action_info.get('msg_type', 'text')
                SendWechatAction(
                    context,
                    account_id,
                    msg,
                    msg_type,
                    row=self.row,
                    converted_row=self.converted_row
                ).do_action()
            elif action_info.get('type') == 'send_dingtalk':
                account_id = int(action_info.get('account_id'))
                msg = action_info.get('default_msg')
                title = action_info.get('default_title')
                msg_type = action_info.get('msg_type', 'text')
                SendDingtalkAction(
                    context,
                    account_id,
                    msg,
                    msg_type,
                    title,
                    row=self.row,
                    converted_row=self.converted_row
                ).do_action()
            elif action_info.get('type') == 'add_record':
                new_row = action_info.get('row')
                # TODO: cant add row
                if not new_row:
                    continue
                AddRowAction(
                    context,
                    new_row
                ).do_action()
            elif action_info.get('type') == 'update_record' and self.row:
                updates = action_info.get('updates')
                UpdateAction(
                    context,
                    updates,
                    row_id=self.row['_id'] or self.converted_row['_id']
                ).do_action()
            elif action_info.get('type') == 'lock_record':
                if self.row:
                    kwargs = {'row_id': self.row['_id']}
                else:
                    filters = self.trigger.get('filters', [])
                    filter_conjunction = self.trigger.get('filter_conjunction', 'And')
                    kwargs = {
                        'filters': filters,
                        'filter_conjunction': filter_conjunction
                    }
                LockRecordAction(context, **kwargs).do_action()
            elif action_info.get('type') == 'link_records':
                if not self.row or self.run_condition != PER_UPDATE:
                    continue
                link_id = action_info.get('link_id')
                linked_table_id = action_info.get('linked_table_id')
                match_conditions = action_info.get('match_conditions')
                LinkRecordsAction(
                    context,
                    link_id,
                    linked_table_id,
                    match_conditions,
                    self.row,
                    self.converted_row
                ).do_action()
            elif action_info.get('type') == 'run_python_script':
                script_name = action_info.get('script_name')
                workspace_id = action_info.get('workspace_id')
                owner = action_info.get('owner')
                org_id = action_info.get('org_id')
                repo_id = action_info.get('repo_id')
                RunPythonScriptAction(
                    context,
                    script_name,
                    workspace_id,
                    owner,
                    org_id,
                    repo_id,
                    converted_row=self.converted_row,
                    operate_from='automation-rule',
                    operator=self.rule_id
                ).do_action()
        except Exception as e:
            logger.exception(e)
            logger.error('rule: %s, data: %s options: %s do actions error: %s', self.rule_id, self.data, self.options, e)
