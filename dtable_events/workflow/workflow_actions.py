import json
import logging

from dtable_events.automations.general_actions import AddRecordToOtherTableAction, BaseContext, NotifyAction, SendEmailAction, \
    SendWechatAction, SendDingtalkAction, UpdateAction, AddRowAction, LockRecordAction, LinkRecordsAction, \
    RunPythonScriptAction
from dtable_events.db import init_db_session_class

logger = logging.getLogger(__name__)


def do_workflow_actions(task_id, node_id, config):
    db_session = init_db_session_class(config)()
    sql = '''
    SELECT dw.dtable_uuid, dw.token, dw.workflow_config, dwt.row_id FROM dtable_workflows dw
    JOIN dtable_workflow_tasks dwt ON dw.id = dwt.dtable_workflow_id
    WHERE dwt.id=:task_id
    '''
    try:
        task_item = db_session.execute(sql, {'task_id': task_id}).fetchone()
        if not task_item:
            return
        dtable_uuid = task_item.dtable_uuid
        workflow_config = json.loads(task_item.workflow_config)
        workflow_token = task_item.token
        table_id = workflow_config.get('table_id')
        workflow_name = workflow_config.get('workflow_name')
        row_id = task_item.row_id
        context = BaseContext(dtable_uuid, table_id, db_session, caller='workflow')
        nodes = workflow_config.get('nodes', [])
        node = None
        for tmp_node in nodes:
            if tmp_node['_id'] == node_id:
                node = tmp_node
                break
        if not node:
            return
        actions = node.get('actions', [])
        converted_row = context.get_converted_row(table_id, row_id)
        if not converted_row:
            return
        for action_info in actions:
            logger.debug('start action: %s', action_info.get('type'))
            try:
                if action_info.get('type') == 'notify':
                    users = action_info.get('users', [])
                    users_column_key = action_info.get('users_column_key')
                    msg = action_info.get('default_msg')
                    NotifyAction(
                        context,
                        users,
                        msg,
                        NotifyAction.NOTIFY_TYPE_WORKFLOW,
                        converted_row=converted_row,
                        users_column_key=users_column_key,
                        workflow_token=workflow_token,
                        workflow_name=workflow_name,
                        workflow_task_id=task_id
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
                        SendEmailAction.SEND_FROM_WORKFLOW,
                        converted_row=converted_row
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
                        converted_row=converted_row
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
                        converted_row=converted_row
                    ).do_action()
                elif action_info.get('type') == 'add_record':
                    new_row = action_info.get('row')
                    logger.debug('new_row: %s', new_row)
                    if not new_row:
                        continue
                    AddRowAction(
                        context,
                        new_row
                    ).do_action()
                elif action_info.get('type') == 'update_record':
                    updates = action_info.get('updates')
                    logger.debug('updates: %s', updates)
                    if not updates:
                        continue
                    UpdateAction(
                        context,
                        updates,
                        row_id
                    ).do_action()
                elif action_info.get('type') == 'lock_record':
                    LockRecordAction(context, row_id=row_id).do_action()
                elif action_info.get('type') == 'link_records':
                    link_id = action_info.get('link_id')
                    linked_table_id = action_info.get('linked_table_id')
                    match_conditions = action_info.get('match_conditions')
                    LinkRecordsAction(
                        context,
                        link_id,
                        linked_table_id,
                        match_conditions,
                        converted_row
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
                        converted_row=converted_row,
                        operate_from=RunPythonScriptAction.OPERATE_FROM_WORKFLOW,
                        operator=workflow_token
                    ).do_action()
                elif action_info.get('type') == 'add_record_to_other_table':
                    row = action_info.get('row')
                    dst_table_id = action_info.get('dst_table_id')
                    logger.debug('row: %s dst_table_id: %s', row, dst_table_id)
                    AddRecordToOtherTableAction(
                        context,
                        dst_table_id,
                        row
                    ).do_action()
            except Exception as e:
                logger.exception(e)
                logger.error('workflow: %s, task: %s node: %s do action: %s error: %s', workflow_token, task_id, node_id, action_info, e)
    except Exception as e:
        logger.exception(e)
        logger.error('task: %s node: %s do actions error: %s', task_id, node_id, e)
    finally:
        db_session.close()
