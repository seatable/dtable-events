"""Connection validation and fixed SeaTable value adaptation."""
import math
import struct
from datetime import datetime, timezone


class InvalidConnection(ValueError):
    pass


SYSTEM_COLUMNS = {
    '_connection_source_key': 'text',
    '_connection_source_modified_time': 'datetime',
}
TARGET_TYPES = {
    'text': 'text', 'float64': 'number', 'float32': 'number', 'int64': 'number',
    'bool': 'boolean', 'datetime': 'datetime', 'single-select': 'single-select',
    'multiple-select': 'multiple-select', 'list': 'list',
}
SOURCE_TYPES = {
    'text': {'text'}, 'long-text': {'text'}, 'url': {'text'}, 'email': {'text'},
    'number': {'number'}, 'rate': {'number'}, 'duration': {'number'},
    'auto-number': {'text'}, 'checkbox': {'boolean'},
    'date': {'datetime'}, 'ctime': {'datetime'}, 'mtime': {'datetime'},
    'single-select': {'single-select', 'text'},
    'multiple-select': {'multiple-select', 'list'}, 'collaborator': {'list'},
    'creator': {'text'}, 'last-modifier': {'text'},
}
VALUE_MAPPING_TYPES = {
    'single-select': {'text', 'single-select'},
    'multiple-select': {'multiple-select', 'list'},
}

FORMULA_TYPES = {
    'string': {'text'}, 'number': {'number'}, 'date': {'datetime'},
    'bool': {'boolean'}, 'array': {'list', 'multiple-select'},
}


def validate_field_mapping(field_mapping, source_table, target_table, value_mapping):
    source_columns = {column['name']: column for column in source_table['columns']
                      if not column['name'].startswith('_') and not column.get('key', '').startswith('_')}
    target_columns = {column['name']: column for column in target_table['columns']}
    for name, storage_type in SYSTEM_COLUMNS.items():
        if name in target_columns and target_columns[name]['type'] != storage_type:
            raise InvalidConnection('Incompatible system field: %s.' % name)
    field_definitions = {}
    for source_name, target_name in field_mapping.items():
        source_column = source_columns.get(source_name)
        target_column = target_columns.get(target_name)
        if source_column is None or target_column is None:
            raise InvalidConnection('Mapping field does not exist: %s → %s.' % (source_name, target_name))
        target_type = TARGET_TYPES.get(target_column['type'])
        source_types = SOURCE_TYPES.get(source_column['type'], set())
        if source_column['type'] == 'formula':
            source_types = FORMULA_TYPES.get((source_column.get('data') or {}).get('result_type'), set())
        if target_name in value_mapping:
            if target_type not in VALUE_MAPPING_TYPES.get(source_column['type'], set()):
                raise InvalidConnection('Value mappings require a select source and compatible target: %s.' % source_name)
        elif target_type not in source_types:
            raise InvalidConnection('Incompatible mapping: %s → %s.' % (source_name, target_name))
        field_definitions[source_name] = (source_column, target_column)
    return field_definitions


def convert_field_value(value, source_column, target_column, value_mapping=None):
    if value is None:
        return None
    if value_mapping is not None and value != '':
        items = value if isinstance(value, list) else [value]
        if not all(isinstance(item, str) for item in items):
            raise ValueError('Invalid value mapping input')
        mapped = [value_mapping.get(item, item) for item in items]
        value = list(dict.fromkeys(mapped)) if isinstance(value, list) else mapped[0]
    kind = TARGET_TYPES[target_column['type']]
    if kind == 'text':
        if isinstance(value, dict) and source_column['type'] == 'long-text' and isinstance(value.get('text'), str):
            return value['text']
        if isinstance(value, str):
            return value
        raise ValueError('Invalid text value')
    if kind == 'list':
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise ValueError('Invalid list value')
        return value
    if kind == 'number':
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError('Invalid number')
        if target_column['type'] == 'int64':
            if isinstance(value, float) and not value.is_integer():
                raise ValueError('Invalid integer')
            integer = int(value)
            if not -(2 ** 63) <= integer < 2 ** 63:
                raise ValueError('Integer out of range')
            return integer
        number = float(value)
        if target_column['type'] == 'float32':
            number = struct.unpack('f', struct.pack('f', number))[0]
            if not math.isfinite(number):
                raise ValueError('Number out of range')
            return number
        if number != value:
            raise ValueError('Number would lose precision')
        return number
    if kind == 'boolean':
        if type(value) is bool:
            return value
        raise ValueError('Invalid boolean')
    if kind == 'datetime':
        if not isinstance(value, str):
            raise ValueError('Invalid date')
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        date_format = (source_column.get('data') or {}).get('format', 'YYYY-MM-DD')
        if len(value) == 10 or (source_column['type'] == 'date' and 'HH:mm' not in date_format):
            return parsed.date().isoformat()
        # Raw query timestamps carry an offset; never guess a browser/process timezone.
        if parsed.tzinfo is None:
            raise ValueError('Source timestamp has no UTC offset')
        return parsed.astimezone(timezone.utc).isoformat()
    if kind == 'single-select' and value == '':
        return value
    values = [value] if kind == 'single-select' else value
    if not isinstance(values, list) or not all(isinstance(item, str) and item for item in values):
        raise ValueError('Invalid select value')
    return value
