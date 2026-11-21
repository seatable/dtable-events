import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


# File storage is not needed to scan automation rules.
with patch.dict('os.environ', {'SEATABLE_LOG_TO_STDOUT': 'true'}), patch.dict(sys.modules, {
        'seaserv': SimpleNamespace(ccnet_api=None, seafile_api=None, USE_GO_FILESERVER=False),
        'seafobj': SimpleNamespace(fs_mgr=None, commit_mgr=None, block_mgr=None),
}):
    from dtable_events.automations import automations_pipeline, entities


class ScheduledAutomationTest(unittest.TestCase):

    def test_spring_forward_scans_daily_rules_using_local_day_boundary(self):
        tz = ZoneInfo('America/New_York')
        engine = create_engine('sqlite://')
        self.addCleanup(engine.dispose)
        redis = Mock()
        stats_manager = Mock()
        stats_manager.is_exceed.return_value = False
        with patch.object(automations_pipeline, 'init_db_session_class', return_value=sessionmaker(bind=engine)), \
                patch.object(automations_pipeline, 'RedisClient', return_value=redis), \
                patch.object(automations_pipeline, 'AutomationsStatsManager', return_value=stats_manager), \
                patch.object(automations_pipeline, 'TIME_ZONE', tz.key), \
                patch.object(BlockingScheduler, 'start', autospec=True) as start:
            pipeline = automations_pipeline.AutomationsPipeline()
            pipeline.scheduled_scan()

        scheduler = start.call_args.args[0]
        scan_job, = scheduler.get_jobs()
        previous_run = scan_job.trigger.get_next_fire_time(None, datetime(2026, 3, 7, 8, 59, tzinfo=tz))
        current_run = scan_job.trigger.get_next_fire_time(None, datetime(2026, 3, 8, 8, 59, tzinfo=tz))
        previous_run_utc = previous_run.astimezone(timezone.utc).replace(tzinfo=None)
        current_run_utc = current_run.astimezone(timezone.utc).replace(tzinfo=None)
        self.assertEqual(current_run_utc - previous_run_utc, timedelta(hours=23))

        with engine.begin() as connection:
            connection.exec_driver_sql('CREATE TABLE workspaces (id INTEGER, owner TEXT, org_id INTEGER)')
            connection.exec_driver_sql('CREATE TABLE dtables (uuid TEXT, workspace_id INTEGER, deleted INTEGER)')
            connection.exec_driver_sql('''
                CREATE TABLE dtable_automation_rules (
                    id INTEGER, run_condition TEXT, `trigger` TEXT, actions TEXT, dtable_uuid TEXT,
                    last_trigger_time DATETIME, is_valid INTEGER, is_pause INTEGER
                )
            ''')
            connection.exec_driver_sql("INSERT INTO workspaces VALUES (1, 'owner', 1)")
            connection.exec_driver_sql("INSERT INTO dtables VALUES ('base', 1, 0)")
            connection.execute(text('''
                INSERT INTO dtable_automation_rules
                VALUES (:id, 'per_day', :trigger, '[]', 'base', :last_run, 1, 0)
            '''), [
                {'id': rule_id, 'trigger': '{"rule_name":"Daily","condition":"run_periodically","notify_hour":9}',
                 'last_run': last_run}
                for rule_id, last_run in [
                    (1, previous_run_utc),
                    (2, None),
                    (3, datetime(2026, 3, 8, 5)),  # Exactly local midnight.
                    (4, datetime(2026, 3, 8, 4, 59)),  # Still the previous local day.
                ]
            ])

        clock = Mock(wraps=datetime)
        clock.now.side_effect = lambda tz: current_run.astimezone(tz)
        clock.utcnow.return_value = current_run_utc
        with patch.object(automations_pipeline, 'TIME_ZONE', tz.key), \
                patch.object(entities, 'TIME_ZONE', tz.key), \
                patch.object(automations_pipeline, 'datetime', clock), \
                patch.object(entities, 'datetime', clock):
            scan_job.func()

        queued = [(queue, json.loads(payload)) for queue, payload in (call.args for call in redis.lpush.call_args_list)]
        self.assertCountEqual([payload['rule_id'] for _, payload in queued], [1, 2, 4])
        for queue, payload in queued:
            self.assertEqual(queue, 'automation_tasks_20')
            self.assertEqual(payload['scheduled_at'], '2026-03-08T09:00:00-04:00')
