"""Read SeaTable and write ScaleDB directly in dtable-events."""
from datetime import datetime, timedelta, timezone
import uuid

from sqlalchemy import text

from dtable_events.app import config
from dtable_events.automations.hasher import AESPasswordHasher
from dtable_events.seadb_connections.mapping import (
    InvalidConnection, SYSTEM_COLUMNS, convert_field_value, validate_field_mapping,
)
from dtable_events.seadb_connections.source_transport import request_source
from dtable_events.utils.seadb_api import SeaDBAPI, get_seadb_table_id, quote_identifier


BATCH_SIZE = 200
SYNC_LOOKBACK = timedelta(hours=1)


class UpstreamUnavailable(Exception):
    pass


class SourcePermissionDenied(InvalidConnection):
    pass


def source_request(method, url, **kwargs):
    try:
        return request_source(method, url, **kwargs)
    except ValueError as error:
        raise InvalidConnection(str(error)) from error


def validate_source_key_index(table):
    """Validate the source key index and return whether it already exists."""
    columns = {column['name']: column for column in table['columns']}
    source_key = columns.get('_connection_source_key') or {}
    if not (source_key.get('data') or {}).get('not_null'):
        raise InvalidConnection('The source key must not allow empty values.')
    key = source_key.get('key')
    has_source_key_index = False
    for index in (table.get('indexes') or {}).values():
        if index.get('is_deleted'):
            continue
        keys = index.get('columns', [])
        if keys == [key]:
            if not index.get('unique'):
                raise InvalidConnection('The source key index must be unique.')
            has_source_key_index = True
    return has_source_key_index


def parse_timestamp(value):
    try:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if not isinstance(value, datetime):
            raise ValueError()
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    except (ValueError, TypeError, OverflowError):
        raise UpstreamUnavailable('Invalid modification time from the source or target.')


def quote_sql_string(value):
    # dtable-db supports quoted SQL strings, but its query API has no parameters.
    return "'%s'" % value.replace('\\', '\\\\').replace("'", "''")


def build_source_query(table, field_names, since_at, sync_before, last_row_id, limit):
    fields = sorted(set(field_names) | {'_id', '_mtime'})
    predicates = ['`_mtime` < %s' % quote_sql_string(sync_before.replace(tzinfo=timezone.utc).isoformat())]
    if since_at:
        predicates.append('`_mtime` >= %s' % quote_sql_string(since_at.replace(tzinfo=timezone.utc).isoformat()))
    if last_row_id:
        predicates.append('`_id` > %s' % quote_sql_string(last_row_id))
    return 'SELECT %s FROM %s WHERE %s ORDER BY `_id` ASC LIMIT %d' % (
        ', '.join(quote_identifier(field) for field in fields), quote_identifier(table['name']),
        ' AND '.join(predicates), limit,
    )


def query_source_rows(api, table, field_mapping, since_at, sync_before, last_row_id='', limit=BATCH_SIZE):
    rows = api.execute_sql(build_source_query(table, field_mapping, since_at, sync_before, last_row_id, limit))
    if not isinstance(rows, list):
        raise UpstreamUnavailable('Invalid source query response.')
    # Converted SQL results use display names even for the built-in mtime column.
    mtime_name = next((column['name'] for column in table.get('columns', []) if column.get('key') == '_mtime'), '_mtime')
    required_fields = set(field_mapping) | {'_mtime'}
    previous_row_id = last_row_id
    for row in rows:
        if (not isinstance(row, dict) or not isinstance(row.get('_id'), str)
                or not row['_id'] or ':' in row['_id'] or row['_id'] <= previous_row_id):
            raise UpstreamUnavailable('Source did not return valid rows in cursor order.')
        if '_mtime' not in row and mtime_name in row:
            row['_mtime'] = row[mtime_name]
        if not required_fields.issubset(row):
            raise UpstreamUnavailable('Source did not return the requested fields.')
        row['_mtime'] = parse_timestamp(row['_mtime'])
        if row['_mtime'] >= sync_before or (since_at and row['_mtime'] < since_at):
            raise UpstreamUnavailable('Source returned rows outside the requested time range.')
        previous_row_id = row['_id']
    return rows


def map_source_row(row, field_mapping, field_definitions, value_mapping):
    result = {}
    for source_name, target_name in field_mapping.items():
        source_column, target_column = field_definitions[source_name]
        try:
            value = row[source_name]
            result[target_name] = convert_field_value(value, source_column, target_column, value_mapping.get(target_name))
        except (ValueError, TypeError, OverflowError):
            raise InvalidConnection('Incompatible value for field %s → %s. Check the source value, value mapping and target type.' % (
                source_name, target_name))
    return result


def build_source_record_key(base_id, table_id, row_id):
    # Base UUID and immutable table ID are resolved before reaching this boundary.
    return '%s:%s:%s' % (base_id, table_id, row_id)


def create_missing_select_options(api, connection, table, rows):
    for column in table['columns']:
        if column['type'] not in ('single-select', 'multiple-select'):
            continue
        data = column.get('data') or {}
        options = data.get('options') or []
        names = {option['name'] for option in options}
        for row in rows:
            value = row.get(column['name'])
            values = [value] if column['type'] == 'single-select' else (value or [])
            for name in values:
                if not name or name in names:
                    continue
                # ScaleDB atomically appends by name and reuses an existing option.
                response = api.add_column_option(connection.database_uuid, connection.config['target']['table_id'], column['name'], name)
                if not isinstance(response, dict) or not isinstance(response.get('option_id'), str) or not response['option_id']:
                    raise UpstreamUnavailable('Unable to create a target select option.')
                options.append({'id': response['option_id'], 'name': name})
                column['data'] = {**data, 'options': options}
                names.add(name)


def write_target_rows(api, connection, table, rows, progress):
    source_keys = [row['_connection_source_key'] for row in rows]
    sql = 'SELECT `_pk`, `_connection_source_key`, `_connection_source_modified_time` FROM %s WHERE `_connection_source_key` IN (%s) LIMIT %d' % (
        quote_identifier(table['name']), ', '.join('?' for _ in source_keys), len(source_keys))
    response = api.query_rows(connection.database_uuid, sql, parameters=source_keys)
    if not isinstance(response, dict) or not isinstance(response.get('results'), list):
        raise UpstreamUnavailable('Invalid target query response.')
    source_key_set = set(source_keys)
    existing_rows = {}
    for item in response['results']:
        if (not isinstance(item, dict) or not isinstance(item.get('_connection_source_key'), str)
                or item['_connection_source_key'] not in source_key_set
                or item.get('_pk') is None or item['_connection_source_key'] in existing_rows):
            raise UpstreamUnavailable('Invalid target row identity.')
        existing_rows[item['_connection_source_key']] = item
    rows_to_insert, rows_to_update = [], []
    for row in rows:
        existing_row = existing_rows.get(row['_connection_source_key'])
        source_modified_time = row['_connection_source_modified_time']
        # Incremental runs skip unchanged rows. Initial syncs also overwrite
        # rows already written during an earlier run.
        if existing_row and connection.last_sync_time and existing_row.get('_connection_source_modified_time') is not None:
            if parse_timestamp(existing_row['_connection_source_modified_time']) >= source_modified_time:
                continue
        payload = {**row, '_connection_source_modified_time': source_modified_time.replace(tzinfo=timezone.utc).isoformat()}
        if existing_row is None:
            rows_to_insert.append(payload)
        else:
            rows_to_update.append({'pk': existing_row['_pk'], 'row': payload})
    table_id = connection.config['target']['table_id']
    create_missing_select_options(api, connection, table, rows_to_insert + [update['row'] for update in rows_to_update])
    if rows_to_insert:
        result = api.insert_rows(connection.database_uuid, table_id, rows_to_insert)
        if not isinstance(result, dict) or result.get('inserted_row_count') != len(rows_to_insert):
            raise UpstreamUnavailable('Target did not acknowledge all inserted rows.')
        progress.inserted_count += len(rows_to_insert)
    if rows_to_update:
        result = api.update_rows(connection.database_uuid, table_id, rows_to_update)
        if not isinstance(result, dict) or result.get('success') is not True:
            raise UpstreamUnavailable('Target did not acknowledge the updates.')
        progress.updated_count += len(rows_to_update)


def parse_source_response(response):
    if response.status_code in (401, 403):
        raise SourcePermissionDenied('Source authorization is unavailable. Create a new connection with a valid base API token.')
    if response.status_code == 404:
        raise InvalidConnection('Source authorization is unavailable. Create a new connection with a valid base API token.')
    if response.status_code != 200:
        raise UpstreamUnavailable('Unable to access the source.')
    try:
        data = response.json()
    except ValueError:
        raise UpstreamUnavailable('Invalid source response.')
    if not isinstance(data, dict):
        raise UpstreamUnavailable('Invalid source response.')
    return data


class SeaTableSourceAPI:
    def __init__(self, source, api_token):
        self.server_url = source['server_url'].rstrip('/')
        self.api_token = api_token
        self.dtable_uuid = source.get('base_id')
        self._auth()

    def _auth(self):
        url = self.server_url + '/api/v2.1/dtable/app-access-token/'
        response = source_request('GET', url, headers={'Authorization': 'Token ' + self.api_token}, timeout=30)
        data = parse_source_response(response)
        try:
            base_id = str(uuid.UUID(data['dtable_uuid']))
        except (KeyError, ValueError, TypeError, AttributeError):
            raise UpstreamUnavailable('Invalid source authorization response.')
        if self.dtable_uuid and base_id != self.dtable_uuid:
            raise InvalidConnection('The API token belongs to a different source base.')
        if (not isinstance(data.get('access_token'), str) or not data['access_token']
                or not isinstance(data.get('dtable_name'), str)):
            raise UpstreamUnavailable('Invalid source authorization response.')
        self.dtable_uuid = base_id
        self.base_name = data['dtable_name']
        self.access_token = data['access_token']
        self.api_url = '%s/api-gateway/api/v2/dtables/%s' % (self.server_url, base_id)

    def _request(self, method, url, **kwargs):
        headers = {'Authorization': 'Token ' + self.access_token}
        response = source_request(method, url, headers=headers, **kwargs)
        if response.status_code in (401, 403):
            self._auth()
            headers = {'Authorization': 'Token ' + self.access_token}
            response = source_request(method, url, headers=headers, **kwargs)
        return parse_source_response(response)

    def get_metadata(self):
        data = self._request('GET', self.api_url + '/metadata/', timeout=30)
        return data.get('metadata', data)

    def execute_sql(self, sql):
        data = self._request('POST', self.api_url + '/sql/', timeout=90, json={
            'sql': sql, 'convert_keys': True, 'convert_date': False,
        })
        if data.get('success') is not True:
            raise UpstreamUnavailable('The source query could not be completed.')
        return data.get('results')


def sync_seatable_connection(db_session_class, connection, progress):
    source_config = connection.config['source']
    target_config = connection.config['target']
    field_mapping = connection.config['field_mapping']
    value_mapping = connection.config.get('value_mapping', {})
    encrypted_token = source_config.get('api_token')
    if not encrypted_token:
        raise InvalidConnection('Source authorization is missing. Create a new connection.')
    try:
        api_token = AESPasswordHasher().decode(encrypted_token)
    except Exception:
        raise InvalidConnection('Unable to read source authorization. Create a new connection.')
    source_api = SeaTableSourceAPI(source_config, api_token)
    base_name = source_api.base_name
    metadata = source_api.get_metadata()
    source_table = next((table for table in metadata['tables'] if table['_id'] == source_config['table_id']), None)
    if not source_table:
        raise InvalidConnection('Source table does not exist.')

    since_at = connection.last_sync_time - SYNC_LOOKBACK if connection.last_sync_time else None
    sync_before = datetime.utcnow()
    with SeaDBAPI(config.SEADB_SERVER_URL, config.SEADB_USER, config.SEADB_PASSWORD) as seadb_api:
        target_metadata = seadb_api.get_base_info(connection.database_uuid)
        target_table = next((table for table in target_metadata['tables']
                             if str(get_seadb_table_id(table)) == str(target_config['table_id'])), None)
        if not target_table:
            raise InvalidConnection('Target table does not exist.')
        field_definitions = validate_field_mapping(field_mapping, source_table, target_table, value_mapping)
        target_columns = {column['name'] for column in target_table['columns']}
        for name in SYSTEM_COLUMNS:
            if name not in target_columns:
                raise InvalidConnection('System field does not exist: %s.' % name)
        if not validate_source_key_index(target_table):
            raise InvalidConnection('The source key index does not exist.')
        last_row_id = ''
        while True:
            source_rows = query_source_rows(source_api, source_table, field_mapping, since_at, sync_before, last_row_id)
            if not source_rows:
                break
            target_rows = []
            for row in source_rows:
                target_row = map_source_row(row, field_mapping, field_definitions, value_mapping)
                target_row.update({
                    '_connection_source_key': build_source_record_key(source_config['base_id'], source_table['_id'], row['_id']),
                    '_connection_source_modified_time': row['_mtime'],
                })
                target_rows.append(target_row)
            previous_write_count = progress.inserted_count + progress.updated_count
            write_target_rows(seadb_api, connection, target_table, target_rows, progress)
            if progress.inserted_count + progress.updated_count != previous_write_count:
                progress.save_status_and_time(connection.last_sync_time)
            last_row_id = source_rows[-1]['_id']
        if (source_config.get('base_name'), source_config.get('table_name'), target_config.get('table_name')) != (
                base_name, source_table['name'], target_table['name']):
            source_config.update(base_name=base_name, table_name=source_table['name'])
            target_config['table_name'] = target_table['name']
            with db_session_class() as session:
                session.execute(text("""UPDATE connections SET config = JSON_SET(config,
                                        '$.source.base_name', :base_name, '$.source.table_name', :source_name,
                                        '$.target.table_name', :target_name) WHERE id = :id"""),
                                {'base_name': base_name, 'source_name': source_table['name'],
                                 'target_name': target_table['name'], 'id': connection.id})
                session.commit()
    return sync_before
