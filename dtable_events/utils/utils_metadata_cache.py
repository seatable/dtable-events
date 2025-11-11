import json
import logging

from dtable_events.app.config import INNER_DTABLE_SERVER_URL
from dtable_events.app.event_redis import redis_cache
from dtable_events.utils import uuid_str_to_36_chars
from dtable_events.utils.dtable_server_api import DTableServerAPI

logger = logging.getLogger(__name__)


def get_key(dtable_uuid):
    dtable_uuid = uuid_str_to_36_chars(dtable_uuid)
    return f'dtable:{dtable_uuid}:metadata'

def get_metadata(dtable_uuid):
    key = get_key(dtable_uuid)
    metadata_str = redis_cache.get(key)
    logger.debug('instant metadata dtable_uuid: %s metadata: %s', dtable_uuid, bool(metadata_str))
    if metadata_str:
        try:
            metadata = json.loads(metadata_str)
            return metadata
        except:
            pass
    dtable_server_api = DTableServerAPI('dtable-events', dtable_uuid, INNER_DTABLE_SERVER_URL)
    metadata = dtable_server_api.get_metadata()
    redis_cache.set(key, json.dumps(metadata), timeout=60)
    return metadata

def clean_metadata(dtable_uuid):
    key = get_key(uuid_str_to_36_chars(dtable_uuid))
    redis_cache.delete(key)
