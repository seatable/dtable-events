"""ScaleDB view export jobs executed by the IO workers."""
import json
import logging
import math
import os
import re
import time
from pathlib import Path
from uuid import UUID
from zoneinfo import ZoneInfo

import requests
from dateutil.parser import isoparse
from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell

from dtable_events.app.config import (
    IO_SERVER_TASK_TIMEOUT, SEADB_SERVER_URL, SEADB_VIEW_EXPORT_CELL_LIMIT, SEADB_VIEW_EXPORT_ROW_LIMIT, TIME_ZONE,
)
from dtable_events.dtable_io.excel import _format_datetime_with_pattern, _format_number_display_string, parse_number_format
from dtable_events.dtable_io.utils import DTABLE_IO_DIR, escape_sheet_name

logger = logging.getLogger(__name__)
EXPORT_ROOT = Path(DTABLE_IO_DIR) / 'export-seadb-view-to-excel'
BATCH_SIZE = 1000
XML_INVALID_CHARACTERS = re.compile(r'[^\x09\x0a\x0d\x20-\ud7ff\ue000-\ufffd\U00010000-\U0010ffff]')
NUMBER_TYPES = ('int32', 'int64', 'float32', 'float64', 'number', 'rate')
DATE_TYPES = ('datetime', 'date')
SELECT_TYPES = ('single-select', 'multiple-select')


def _query_rows(session, url, sql, parameters, timeout):
    response = session.post(url, json={
        'sql': sql,
        'parameters': parameters,
        'convert_keys': True,
    }, timeout=timeout, allow_redirects=False)
    if response.status_code == 408:
        raise TimeoutError('SeaDB query timed out')
    if response.status_code != 200:
        raise RuntimeError(f'SeaDB query returned HTTP {response.status_code}')
    result = response.json()
    if result.get('success') is not True or not isinstance(result.get('results'), list):
        raise ValueError('Invalid SeaDB query response')
    return result['results']


def query_pages(payload, row_limit, deadline, state):
    url = f"{SEADB_SERVER_URL.rstrip('/')}/api/v1/{payload['database_uuid']}/query"
    parameters = payload['parameters']
    cursor_sql = payload['cursor_sql']
    sql = payload['select_sql']
    last_pk = None
    offset = 0
    with requests.Session() as session:
        session.headers['Authorization'] = f"Bearer {payload['read_token']}"
        if payload.get('count_sql'):
            remaining_time = deadline - time.time()
            if remaining_time > 0:
                try:
                    counts = _query_rows(session, url, payload['count_sql'], parameters, min(5, remaining_time))
                    if len(counts) == 1 and isinstance(counts[0], dict):
                        count = counts[0].get('total_row_count')
                        if type(count) is int and count >= 0:
                            state['total_row_count'] = min(count, row_limit)
                except (requests.RequestException, RuntimeError, TimeoutError, ValueError):
                    # A count is only needed for the progress percentage.
                    logger.info('Could not count ScaleDB export rows')
        while offset <= row_limit:
            remaining_time = deadline - time.time()
            if remaining_time <= 0:
                raise TimeoutError('ScaleDB export timed out')
            # One extra row tells the caller whether the export was truncated.
            limit = min(BATCH_SIZE, row_limit + 1 - offset)
            if cursor_sql and last_pk is not None:
                # SeaDB decodes JSON numbers into float64. Keep the int64 cursor exact.
                before_cursor, _, after_cursor = cursor_sql.rpartition('?')
                sql = f'{before_cursor}{last_pk:d}{after_cursor}'
            start = 0 if cursor_sql else offset
            rows = _query_rows(session, url, f'{sql} LIMIT {start}, {limit}', parameters, min(35, remaining_time))
            if not rows:
                return
            if len(rows) > limit:
                raise ValueError('SeaDB returned more rows than requested')
            if cursor_sql:
                next_pk = rows[-1]['_pk']
                if type(next_pk) is not int or (last_pk is not None and next_pk <= last_pk):
                    raise ValueError('SeaDB primary key cursor did not advance')
                last_pk = next_pk
            offset += len(rows)
            yield rows


def text_cell(sheet, value):
    value = XML_INVALID_CHARACTERS.sub('', str(value))[:32767]
    cell = WriteOnlyCell(sheet, value=value)
    cell.data_type = 's'
    return cell


def option_names(column):
    options = (column.get('data') or {}).get('options') or []
    names = {str(option['id']): option['name'] for option in options if option.get('id') is not None}
    names.update({option['name']: option['name'] for option in options})
    return names


def display_value(value, names):
    if value is None:
        return ''
    if isinstance(value, list):
        return ', '.join(display_value(item, names) for item in value)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return str(names.get(str(value), value))


def _get_result_column(column):
    data = column.get('data') or {}
    if column['type'] != 'link-formula' or data.get('formula') != 'rollup':
        return column

    method = data.get('summary_method')
    if method == 'count':
        return {**column, 'type': 'int64', 'data': {}}

    target = column.get('linked_display_column')
    if method in ('sum', 'average'):
        target = target or {'type': 'float64'}
    elif method not in ('min', 'max'):
        return column

    if target:
        return {**column, 'type': target['type'], 'data': target.get('data') or {}}
    return column


def _format_decimal(value):
    # Fixed-point formatting also handles numbers represented in scientific notation.
    if not math.isfinite(value):
        return '0'
    decimals = f'{value:.8f}'.rstrip('0').partition('.')[2]
    if not decimals:
        return '0'
    return '0.' + '0' * len(decimals)


def _build_formatted_excel_cell(sheet, value, number_format):
    cell = WriteOnlyCell(sheet, value=value)
    cell.number_format = number_format
    return cell


def _parse_local_date(value, timezone):
    try:
        date = isoparse(value)
    except (ValueError, OverflowError):
        return None
    if date.tzinfo is not None:
        date = date.astimezone(timezone).replace(tzinfo=None)
    return date


def _format_lookup_value(value, target, names, timezone):
    if isinstance(value, dict) and 'display_value' in value:
        value = value['display_value']
    if isinstance(value, list):
        values = (_format_lookup_value(item, target, names, timezone) for item in value)
        return ', '.join(filter(None, values))

    column_type = target['type']
    data = target.get('data') or {}
    if column_type in NUMBER_TYPES and type(value) in (int, float):
        if (isinstance(value, int) and abs(value) >= 10**15) or not math.isfinite(value):
            return str(value)
        return _format_number_display_string(value, data)
    if column_type in DATE_TYPES and isinstance(value, str) and value:
        date = _parse_local_date(value, timezone)
        if date is not None:
            return _format_datetime_with_pattern(date, data.get('format'))
    return display_value(value, names)


def _get_option_names(column):
    target = column.get('linked_display_column')
    if column['type'] == 'auto-link':
        return option_names(target or column)
    if column['type'] in SELECT_TYPES:
        return option_names(column)
    if column['type'] == 'link-formula' and (column.get('data') or {}).get('formula') == 'lookup':
        if target and target['type'] in SELECT_TYPES:
            return option_names(target)
    return {}


def value_cell(sheet, column, value, names, timezone):
    if value is None:
        return None

    column_type = column['type']
    data = column.get('data') or {}
    target = column.get('linked_display_column')
    if column_type == 'link-formula' and data.get('formula') == 'lookup' and target:
        formatted = _format_lookup_value(value, target, names, timezone)
        return text_cell(sheet, formatted) if formatted else None
    if column_type in ('bool', 'checkbox') and isinstance(value, bool):
        return value
    if column_type in NUMBER_TYPES and type(value) in (int, float):
        if isinstance(value, int) and abs(value) >= 10**15:
            return text_cell(sheet, value)
        if not math.isfinite(value):
            return text_cell(sheet, value)
        number_format = parse_number_format(data, value, decimal_formatter=_format_decimal)
        return _build_formatted_excel_cell(sheet, value, number_format)
    if column_type in DATE_TYPES and isinstance(value, str) and value:
        date = _parse_local_date(value, timezone)
        if date is None:
            return text_cell(sheet, value)
        return _build_formatted_excel_cell(sheet, date, data.get('format') or 'yyyy-mm-dd')
    if column_type in ('auto-link', 'link-formula') and isinstance(value, list):
        value = [item.get('display_value') if isinstance(item, dict) else item for item in value]
    return text_cell(sheet, display_value(value, names))


def export_seadb_view_to_excel(payload, task_id, tasks_status_map, timeout=IO_SERVER_TASK_TIMEOUT):
    deadline = time.time() + timeout
    database_id = str(UUID(payload['database_uuid']))
    target_dir = EXPORT_ROOT / database_id
    temporary = target_dir / f'.{task_id}.tmp'
    workbook = None
    sheet = None
    state = {
        'status': 'running',
        'handled_row_count': 0,
        'filename': payload['filename'],
    }
    tasks_status_map[task_id] = dict(state)
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        columns = [_get_result_column(column) for column in payload['columns']]
        row_limit = min(SEADB_VIEW_EXPORT_ROW_LIMIT, SEADB_VIEW_EXPORT_CELL_LIMIT // len(columns), 1048575)
        if len(columns) > 16384 or row_limit < 1:
            raise ValueError('ScaleDB export exceeds the workbook column or cell limit')
        state['row_limit'] = row_limit
        names = [_get_option_names(column) for column in columns]
        timezone = ZoneInfo(TIME_ZONE)
        workbook = Workbook(write_only=True)
        title = escape_sheet_name(payload['sheet_name'])
        title = XML_INVALID_CHARACTERS.sub('', title)[:31]
        sheet = workbook.create_sheet(title or 'View')
        sheet.append([text_cell(sheet, column['name']) for column in columns])
        truncated = False
        for rows in query_pages(payload, row_limit, deadline, state):
            remaining = row_limit - state['handled_row_count']
            for row in rows[:remaining]:
                sheet.append([
                    value_cell(sheet, column, row.get(column['name']), name_map, timezone)
                    for column, name_map in zip(columns, names)
                ])
            state['handled_row_count'] += min(len(rows), remaining)
            tasks_status_map[task_id] = dict(state)
            if len(rows) > remaining:
                truncated = True
                break
        workbook.save(temporary)
        if time.time() >= deadline:
            raise TimeoutError('ScaleDB export timed out')
        os.replace(temporary, target_dir / payload['filename'])
        state.update(status='success', is_truncated=truncated, total_row_count=state['handled_row_count'])
        tasks_status_map[task_id] = dict(state)
    except Exception as error:
        logger.exception('ScaleDB export %s failed', task_id)
        temporary.unlink(missing_ok=True)
        error_code = 'timeout' if isinstance(error, (TimeoutError, requests.Timeout)) else 'export_failed'
        state.update(status='failed', error_code=error_code)
        tasks_status_map[task_id] = dict(state)
    finally:
        if sheet is not None and sheet._writer is not None:
            # A failed write-only workbook may still own the uncompressed sheet file.
            try:
                if not sheet.closed:
                    sheet.close()
            finally:
                if os.path.exists(sheet._writer.out):
                    sheet._writer.cleanup()
        if workbook is not None:
            workbook.close()
