import json
import logging
import time
from threading import Thread, Event

from sqlalchemy import text

from dtable_events.app.event_redis import RedisClient
from dtable_events.automations.general_actions import ActionInvalid, AddRecordToOtherTableAction, BaseContext, NotifyAction, SendEmailAction, \
    SendWechatAction, SendDingtalkAction, UpdateAction, AddRowAction, LockRecordAction, LinkRecordsAction, \
    RunPythonScriptAction
from dtable_events.db import init_db_session_class

logger = logging.getLogger(__name__)


def do_workflow_actions(task_id, node_id, db_session):
    sql = '''
    SELECT dw.dtable_uuid, dw.token, dw.workflow_config, dwt.row_id FROM dtable_workflows dw
    JOIN dtable_workflow_tasks dwt ON dw.id = dwt.dtable_workflow_id
    WHERE dwt.id=:task_id
    '''
    task_item = db_session.execute(text(sql), {'task_id': task_id}).fetchone()
    if not task_item:
        return
    dtable_uuid = task_item.dtable_uuid
    workflow_config = json.loads(task_item.workflow_config)
    workflow_token = task_item.token
    table_id = workflow_config.get('table_id')
    workflow_name = workflow_config.get('workflow_name')
    row_id = task_item.row_id
    try:
        context = BaseContext(dtable_uuid, table_id, db_session, caller='workflow')
    except Exception as e:
        logger.error('task: %s node: %s dtable_uuid: %s context error: %s', task_id, node_id, dtable_uuid, e)
        return
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
                    users_column_key=users_column_key,
                    workflow_token=workflow_token,
                    workflow_name=workflow_name,
                    workflow_task_id=task_id
                ).do_action_with_row(converted_row)
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
                    SendEmailAction.SEND_FROM_WORKFLOW
                ).do_action_with_row(converted_row)
            elif action_info.get('type') == 'send_wechat':
                account_id = int(action_info.get('account_id'))
                msg = action_info.get('default_msg')
                msg_type = action_info.get('msg_type', 'text')
                SendWechatAction(
                    context,
                    account_id,
                    msg,
                    msg_type
                ).do_action_with_row(converted_row)
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
                    title
                ).do_action_with_row(converted_row)
            elif action_info.get('type') == 'add_record':
                new_row = action_info.get('row')
                logger.debug('new_row: %s', new_row)
                if not new_row:
                    continue
                AddRowAction(
                    context,
                    new_row
                ).do_action_without_row()
            elif action_info.get('type') == 'update_record':
                updates = action_info.get('updates')
                logger.debug('updates: %s', updates)
                if not updates:
                    continue
                UpdateAction(
                    context,
                    updates
                ).do_action_with_row(converted_row)
            elif action_info.get('type') == 'lock_record':
                LockRecordAction(context).do_action_with_row(converted_row)
            elif action_info.get('type') == 'link_records':
                link_id = action_info.get('link_id')
                linked_table_id = action_info.get('linked_table_id')
                match_conditions = action_info.get('match_conditions')
                LinkRecordsAction(
                    context,
                    link_id,
                    linked_table_id,
                    match_conditions
                ).do_action_with_row(converted_row)
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
                    operate_from=RunPythonScriptAction.OPERATE_FROM_WORKFLOW,
                    operator=workflow_token
                ).do_action_with_row(converted_row)
            elif action_info.get('type') == 'add_record_to_other_table':
                row = action_info.get('row')
                dst_table_id = action_info.get('dst_table_id')
                logger.debug('row: %s dst_table_id: %s', row, dst_table_id)
                AddRecordToOtherTableAction(
                    context,
                    dst_table_id,
                    row
                ).do_action_without_row()
        except ActionInvalid as e:
            logger.error('task_id: %s node_id: %s action: %s invalid', task_id, node_id, action_info)
        except Exception as e:
            logger.exception(e)
            logger.error('workflow: %s, task: %s node: %s do action: %s error: %s', workflow_token, task_id, node_id, action_info, e)


class WorkflowActionsHandler(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self._finished = Event()
        self._db_session_class = init_db_session_class(config)
        self._redis_client = RedisClient(config)
    
    def run(self):
        logger.info('Starting handle workflow actions...')
        subscriber = self._redis_client.get_subscriber('workflow-actions')

        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    sub_data = json.loads(message['data'])
                    session = self._db_session_class()
                    task_id = sub_data['task_id']
                    node_id = sub_data['node_id']
                    try:
                        do_workflow_actions(task_id, node_id, session)
                    except Exception as e:
                        logger.exception(e)
                        logger.error('task: %s node: %s do actions error: %s', task_id, node_id, e)
                    finally:
                        session.close()
                else:
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get workflow-actions message: %s', e)
                subscriber = self._redis_client.get_subscriber('workflow-actions')
