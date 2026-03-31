from copy import deepcopy
from datetime import datetime, date
from dataclasses import dataclass, field


PER_DAY = 'per_day'
PER_WEEK = 'per_week'
PER_UPDATE = 'per_update'
PER_MONTH = 'per_month'
CRON_CONDITIONS = (PER_DAY, PER_WEEK, PER_MONTH)
ALL_CONDITIONS = (PER_DAY, PER_WEEK, PER_MONTH, PER_UPDATE)

CONDITION_ROWS_MODIFIED = 'rows_modified'
CONDITION_ROWS_ADDED = 'rows_added'
CONDITION_FILTERS_SATISFY = 'filters_satisfy'
CONDITION_PERIODICALLY = 'run_periodically'
CONDITION_PERIODICALLY_BY_CONDITION = 'run_periodically_by_condition'

QUEUE_AUTOMATION_TASKS_10 = 'automation_tasks_10'
QUEUE_AUTOMATION_TASKS_20 = 'automation_tasks_20'
QUEUE_AUTOMATION_TASKS_30 = 'automation_tasks_30'
QUEUE_AUTOMATION_TASKS_DEFAULT = QUEUE_AUTOMATION_TASKS_20


class AutomationTask:

    def __init__(self, rule_id, run_condition, trigger, actions, dtable_uuid, org_id, owner, data, with_test, task_id=None):
        self.rule_id = rule_id
        self.run_condition = run_condition
        self.trigger = trigger
        self.actions = actions
        self.rule_name = trigger['rule_name']
        self.dtable_uuid = dtable_uuid
        self.org_id = org_id
        self.owner = owner
        self.data = data
        self.with_test = with_test
        self.task_id = task_id
        self.warnings = []

    def to_dict(self):
        return {key: value for key, value in self.__dict__.items() if key not in ['trigger', 'actions']}

    def append_warning(self, warning):
        self.warnings.append(warning)

    def can_do_actions(self):
        if self.trigger.get('condition') not in (CONDITION_FILTERS_SATISFY, CONDITION_PERIODICALLY, CONDITION_ROWS_ADDED, CONDITION_PERIODICALLY_BY_CONDITION):
            return False

        if self.trigger.get('condition') == CONDITION_ROWS_ADDED:
            if self.data.get('op_type') not in ['insert_row', 'append_rows', 'insert_rows']:
                return False

        if self.trigger.get('condition') in [CONDITION_FILTERS_SATISFY, CONDITION_ROWS_MODIFIED]:
            if self.data.get('op_type') not in ['modify_row', 'modify_rows', 'add_link', 'update_links', 'update_rows_links', 'remove_link', 'move_group_rows']:
                return False

        if self.run_condition == PER_UPDATE:
            return True

        if self.run_condition in CRON_CONDITIONS:
            cur_datetime = datetime.now()
            cur_hour = cur_datetime.hour
            cur_week_day = cur_datetime.isoweekday()
            cur_month_day = cur_datetime.day
            if self.run_condition == PER_DAY:
                trigger_hour = self.trigger.get('notify_hour', 12)
                if cur_hour != trigger_hour:
                    return False
            elif self.run_condition == PER_WEEK:
                trigger_hour = self.trigger.get('notify_week_hour', 12)
                trigger_day = self.trigger.get('notify_week_day', 7)
                if cur_hour != trigger_hour or cur_week_day != trigger_day:
                    return False
            else:
                trigger_hour = self.trigger.get('notify_month_hour', 12)
                trigger_day = self.trigger.get('notify_month_day', 1)
                if cur_hour != trigger_hour or cur_month_day != trigger_day:
                    return False
            return True

        return False

    def get_priority_queue(self):
        if self.with_test:
            return QUEUE_AUTOMATION_TASKS_30
        for action in self.actions:
            if action['type'] in [
                'calculate_accumulated_value',
                'calculate_delta',
                'calculate_rank',
                'calculate_percentage',
                'lookup_and_copy',
                'extract_user_name',
                'convert_page_to_pdf',
                'convert_document_to_pdf_and_send',
                'run_ai'
            ]:
                return QUEUE_AUTOMATION_TASKS_10
        return QUEUE_AUTOMATION_TASKS_DEFAULT


@dataclass
class AutomationResult:
    rule_id: int
    rule_name: str
    dtable_uuid: str
    run_condition: str
    org_id: int
    owner: str
    with_test: bool

    data: dict = None

    task_id: str = None

    io_task_id: str = None

    is_exceed_system_resource_limit: bool = False

    run_time: float = 0.0
    trigger_time: datetime = None
    trigger_date: date = None

    success: bool = True
    warnings: list = field(default_factory=list)

    is_valid: bool = True
    invalid_type: str = None

    def to_dict(self):
        self_dict = deepcopy(self.__dict__)
        if self_dict['trigger_time']:
            self_dict['trigger_time'] = self_dict['trigger_time'].isoformat()
        if self_dict['trigger_date']:
            self_dict['trigger_date'] = self_dict['trigger_date'].isoformat()
        return self_dict
