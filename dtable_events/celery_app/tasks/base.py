from celery import Task, current_app


class DatabaseTask(Task):
    abstract = True

    def __init__(self):
        super().__init__()
        self.db_session = None

    def before_start(self, task_id, args, kwargs):
        self.db_session = current_app.db_session_class()

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        self.db_session.close()
