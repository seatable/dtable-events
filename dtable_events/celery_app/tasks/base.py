from celery import Task

from dtable_events.celery_app.app import get_session_class


class DatabaseTask(Task):
    _db_session = None

    @property
    def db_session(self):
        if self._db_session is None:
            self._db_session = get_session_class()()
        return self._db_session

    def after_return(self, *args, **kwargs):
        if self._db_session is not None:
            self._db_session.close()
            self._db_session = None
