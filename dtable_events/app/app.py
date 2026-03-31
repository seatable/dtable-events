# -*- coding: utf-8 -*-
import time

from dtable_events.activities.handlers import MessageHandler
from dtable_events.app.stats_sender import StatsSender
from dtable_events.statistics.counter import UserActivityCounter
from dtable_events.dtable_io.dtable_io_server import DTableIOServer
from dtable_events.tasks.instant_notices_sender import InstantNoticeSender
from dtable_events.tasks.email_notices_sender import EmailNoticesSender
from dtable_events.tasks.dtables_cleaner import DTablesCleaner
from dtable_events.tasks.dtable_updates_sender import DTableUpdatesSender
from dtable_events.tasks.dtable_real_time_rows_counter import DTableRealTimeRowsCounter
from dtable_events.tasks.ldap_syncer import LDAPSyncer
from dtable_events.tasks.dtable_asset_trash_cleaner import DTableAssetTrashCleaner
from dtable_events.tasks.license_expiring_notices_sender import LicenseExpiringNoticesSender
from dtable_events.tasks.universal_app_auto_buckup import UniversalAppAutoBackup
from dtable_events.tasks.virus_scanner import VirusScanner
from dtable_events.notification_rules.handler import NotificationRuleHandler
from dtable_events.notification_rules.dtable_notification_rules_scanner import DTableNofiticationRulesScanner
from dtable_events.automations.automations_pipeline import AutomationsPipeline
from dtable_events.webhook.webhook import Webhooker
from dtable_events.common_dataset.common_dataset_syncer import CommonDatasetSyncer
from dtable_events.tasks.big_data_storage_stats_worker import BigDataStorageStatsWorker
from dtable_events.tasks.clean_db_records_worker import CleanDBRecordsWorker
from dtable_events.data_sync.data_syncer import DataSyncer
from dtable_events.workflow.workflow_actions import WorkflowActionsHandler
from dtable_events.workflow.workflow_schedules_scanner import WorkflowSchedulesScanner
from dtable_events.convert_page.manager import get_playwright_manager
from dtable_events.api_calls.api_calls_counter import APICallsCounter
from dtable_events.tasks.dtable_file_access_log_cleaner import DTableFileAccessLogCleaner
from dtable_events.activities.dtable_update_handler import DTableUpdateHander
from dtable_events.activities.dtable_update_cache_manager import DTableUpdateCacheManager
from dtable_events.tasks.metrics import MetricManager
from dtable_events.tasks.ai_stats_worker import AIStatsWorker


class App(object):
    def __init__(self, task_mode):
        self._enable_foreground_tasks = task_mode.enable_foreground_tasks
        self._enable_background_tasks = task_mode.enable_background_tasks
        
        self.dtable_update_cache = DTableUpdateCacheManager()

        self._stats_sender = StatsSender()

        # automations pipeline, to put test tasks to redis for foreground mode and to put real tasks to redis for background mode
        # but not necessary to start in foreground mode
        self._automations_pipeline = AutomationsPipeline()

        if self._enable_foreground_tasks:
            self._dtable_io_server = DTableIOServer(self)
            self._playwright_manager = get_playwright_manager()

        if self._enable_background_tasks:
            # redis client subscriber
            self._message_handler = MessageHandler(self)
            self._notification_rule_handler = NotificationRuleHandler()
            self._user_activity_counter = UserActivityCounter()
            self._dtable_real_time_rows_counter = DTableRealTimeRowsCounter()
            self._workflow_actions_handler = WorkflowActionsHandler()
            self._webhooker = Webhooker()
            self._api_calls_counter = APICallsCounter()
            self._metric_manager = MetricManager()
            self._universal_app_auto_backup = UniversalAppAutoBackup()
            # cron jobs
            self._instant_notices_sender = InstantNoticeSender()
            self._email_notices_sender = EmailNoticesSender()
            self._dtables_cleaner = DTablesCleaner()
            self._dtable_updates_sender = DTableUpdatesSender()
            self._dtable_notification_rules_scanner = DTableNofiticationRulesScanner()
            self._ldap_syncer = LDAPSyncer()
            self._common_dataset_syncer = CommonDatasetSyncer(self)
            self._big_data_storage_stats_worker = BigDataStorageStatsWorker()
            self._clean_db_records_worker = CleanDBRecordsWorker()
            self._data_sync = DataSyncer()
            self._workflow_schedule_scanner = WorkflowSchedulesScanner()
            self._dtable_asset_trash_cleaner = DTableAssetTrashCleaner()
            self._license_expiring_notices_sender = LicenseExpiringNoticesSender()
            self._dtable_access_log_cleaner = DTableFileAccessLogCleaner()
            self._dtable_update_handler = DTableUpdateHander(self)
            # interval
            self._virus_scanner = VirusScanner()
            # ai stats, listen redis and cron
            self.ai_stats_worker = AIStatsWorker()

    def serve_forever(self):


        if self._enable_foreground_tasks:
            self._playwright_manager.start()                 # always True
            self._dtable_io_server.start()                   # always True

        if self._enable_background_tasks:
            # redis client subscriber
            self._metric_manager.start()                     # always True, ready to collect metrics
            self._message_handler.start()                    # always True
            self._notification_rule_handler.start()          # always True
            self._user_activity_counter.start()              # always True
            self._dtable_real_time_rows_counter.start()      # default True
            self._workflow_actions_handler.start()           # always True
            self._webhooker.start()                          # always True
            self._api_calls_counter.start()                  # always True
            self._universal_app_auto_backup.start()          # always True
            # cron jobs
            self._instant_notices_sender.start()             # default True
            self._email_notices_sender.start()               # default True
            self._dtables_cleaner.start()                    # default True
            self._dtable_updates_sender.start()              # default True
            self._dtable_notification_rules_scanner.start()  # default True
            self._ldap_syncer.start()                        # default False
            self._common_dataset_syncer.start()              # default True
            self._big_data_storage_stats_worker.start()      # always True
            self._clean_db_records_worker.start()            # default True
            self._data_sync.start()                          # default True
            self._workflow_schedule_scanner.start()          # default True
            self._dtable_asset_trash_cleaner.start()         # always True
            self._license_expiring_notices_sender.start()    # always True
            self._dtable_access_log_cleaner.start()          # always True
            self._dtable_update_handler.start()              # always True
            # interval
            self._virus_scanner.start()                      # default False
            # ai stats, listen redis and cron
            self.ai_stats_worker.start()                     # default True
            # automations pipeline
            self._automations_pipeline.start()               # always True

        while True:
            time.sleep(60)
