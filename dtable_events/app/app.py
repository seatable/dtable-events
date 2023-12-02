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
from dtable_events.automations.handler import AutomationRuleHandler
from dtable_events.automations.dtable_automation_rules_scanner import DTableAutomationRulesScanner
from dtable_events.automations.auto_rules_stats_updater import auto_rules_stats_updater
from dtable_events.webhook.webhook import Webhooker
from dtable_events.common_dataset.common_dataset_syncer import CommonDatasetSyncer
from dtable_events.tasks.big_data_storage_stats_worker import BigDataStorageStatsWorker
from dtable_events.tasks.clean_db_records_worker import CleanDBRecordsWorker
from dtable_events.data_sync.data_syncer import DataSyncer
from dtable_events.workflow.workflow_actions import WorkflowActionsHandler
from dtable_events.workflow.workflow_schedules_scanner import WorkflowSchedulesScanner
from dtable_events.convert_page.manager import conver_page_to_pdf_manager
from dtable_events.api_calls.api_calls_counter import APICallsCounter
from dtable_events.tasks.dtable_file_access_log_cleaner import DTableFileAccessLogCleaner
from dtable_events.activities.dtable_update_handler import DTableUpdateHander
from dtable_events.activities.dtable_update_cache_manager import DTableUpdateCacheManager
from dtable_events.tasks.metrics import MetricManager
from dtable_events.tasks.ai_stats_worker import AIStatsWorker


class App(object):
    def __init__(self, config, seafile_config, task_mode):
        self._enable_foreground_tasks = task_mode.enable_foreground_tasks
        self._enable_background_tasks = task_mode.enable_background_tasks
        
        self.dtable_update_cache = DTableUpdateCacheManager()

        self._stats_sender = StatsSender(config)

        if self._enable_foreground_tasks:
            self._dtable_io_server = DTableIOServer(self, config)

        if self._enable_background_tasks:
            # redis client subscriber
            self._message_handler = MessageHandler(self, config)
            self._notification_rule_handler = NotificationRuleHandler(config)
            self._automation_rule_handler = AutomationRuleHandler(config)
            self._user_activity_counter = UserActivityCounter(config)
            self._dtable_real_time_rows_counter = DTableRealTimeRowsCounter(config)
            self._workflow_actions_handler = WorkflowActionsHandler(config)
            self._webhooker = Webhooker(config)
            self._api_calls_counter = APICallsCounter(config)
            self._metric_manager = MetricManager(config)
            self._universal_app_auto_backup = UniversalAppAutoBackup(config)
            # cron jobs
            self._instant_notices_sender = InstantNoticeSender(config)
            self._email_notices_sender = EmailNoticesSender(config)
            self._dtables_cleaner = DTablesCleaner(config)
            self._dtable_updates_sender = DTableUpdatesSender(config)
            self._dtable_notification_rules_scanner = DTableNofiticationRulesScanner(config)
            self._dtable_automation_rules_scanner = DTableAutomationRulesScanner(config)
            self._ldap_syncer = LDAPSyncer(config)
            self._common_dataset_syncer = CommonDatasetSyncer(self, config)
            self._big_data_storage_stats_worker = BigDataStorageStatsWorker(config)
            self._clean_db_records_worker = CleanDBRecordsWorker(config)
            self._data_sync = DataSyncer(config)
            self._workflow_schedule_scanner = WorkflowSchedulesScanner(config)
            self._dtable_asset_trash_cleaner = DTableAssetTrashCleaner(config)
            self._license_expiring_notices_sender = LicenseExpiringNoticesSender()
            self._dtable_access_log_cleaner = DTableFileAccessLogCleaner(config)
            self._dtable_update_handler = DTableUpdateHander(self, config)
            # interval
            self._virus_scanner = VirusScanner(config, seafile_config)
            # convert pdf manager
            conver_page_to_pdf_manager.init(config)
            # ai stats, listen redis and cron
            self.ai_stats_worker = AIStatsWorker(config)
            # automation rules statistics updater
            auto_rules_stats_updater.init(config)

    def serve_forever(self):

        if self._enable_foreground_tasks:
            self._dtable_io_server.start()

        if self._enable_background_tasks:
            # redis client subscriber
            self._metric_manager.start()                     # always True, ready to collect metrics
            self._message_handler.start()                    # always True
            self._notification_rule_handler.start()          # always True
            self._automation_rule_handler.start()            # always True
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
            self._dtable_automation_rules_scanner.start()    # default True
            self._ldap_syncer.start()                        # default False
            self._common_dataset_syncer.start()              # default True
            self._big_data_storage_stats_worker.start()      # always True
            self._clean_db_records_worker.start()            # default false
            self._data_sync.start()                          # default True
            self._workflow_schedule_scanner.start()          # default True
            self._dtable_asset_trash_cleaner.start()         # always True
            self._license_expiring_notices_sender.start()    # always True
            self._dtable_access_log_cleaner.start()          # always True
            self._dtable_update_handler.start()              # always True
            # interval
            self._virus_scanner.start()                      # default False
            # convert pdf manager
            conver_page_to_pdf_manager.start()               # always True
            # ai stats, listen redis and cron
            self.ai_stats_worker.start()                     # default False
            # automation rules statistics updater
            auto_rules_stats_updater.start()

        while True:
            time.sleep(60)
