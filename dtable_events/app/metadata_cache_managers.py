import json
import logging

from dtable_events.app.event_redis import redis_cache
from dtable_events.utils import uuid_str_to_36_chars, get_inner_dtable_server_url
from dtable_events.utils.dtable_server_api import DTableServerAPI

logger = logging.getLogger(__name__)
dtable_server_url = get_inner_dtable_server_url()


class BaseMetadataCacheManager:

    def request_metadata(self, dtable_uuid):
        dtable_uuid = uuid_str_to_36_chars(dtable_uuid)
        dtable_server_api = DTableServerAPI('dtable-events', dtable_uuid, dtable_server_url)
        metadata = dtable_server_api.get_metadata()
        return metadata

    def get_metadata(self, dtable_uuid):
        return self.request_metadata(dtable_uuid)

    def clean_metadata(self, dtable_uuid):
        pass


class RuleIntentMetadataCacheManger(BaseMetadataCacheManager):

    def get_key(self, dtable_uuid):
        dtable_uuid = uuid_str_to_36_chars(dtable_uuid)
        return f'dtable:{dtable_uuid}:intent-metadata'

    def get_metadata(self, dtable_uuid):
        key = self.get_key(dtable_uuid)
        metadata_str = redis_cache.get(key)
        logger.debug('intent metadata dtable_uuid: %s metadata: %s', dtable_uuid, bool(metadata_str))
        if metadata_str:
            try:
                metadata = json.loads(metadata_str)
                return metadata
            except:
                pass
        metadata = self.request_metadata(dtable_uuid)
        redis_cache.set(key, json.dumps(metadata), timeout=60)
        return metadata

    def clean_metadata(self, dtable_uuid):
        key = self.get_key(uuid_str_to_36_chars(dtable_uuid))
        redis_cache.delete(key)



class RuleIntervalMetadataCacheManager(BaseMetadataCacheManager):

    def __init__(self):
        self.metadatas_dict = {}

    def get_metadata(self, dtable_uuid):
        metadata = self.metadatas_dict.get(dtable_uuid)
        logger.debug('interval metadata dtable_uuid: %s metadata: %s', dtable_uuid, bool(metadata))
        if metadata:
            return metadata
        metadata = self.request_metadata(dtable_uuid)
        self.metadatas_dict[uuid_str_to_36_chars(dtable_uuid)] = metadata
        return metadata

    def clean_metadata(self, dtable_uuid):
        self.metadatas_dict.pop(uuid_str_to_36_chars(dtable_uuid), None)
