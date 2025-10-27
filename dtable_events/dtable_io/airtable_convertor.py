# -*- coding: utf-8 -*-
import json
import re
import time
import random
import requests
import logging
import urllib.parse
from datetime import datetime

from dtable_events.utils.constants import ColumnTypes


HREF_REG = r'\[.+\]\(\S+\)|<img src=\S+.+\/>|!\[\]\(\S+\)|<\S+>'
LINK_REG_1 = r'^\[.+\]\((\S+)\)'
LINK_REG_2 = r'^<(\S+)>'
IMAGE_REG_1 = r'^<img src="(\S+)" .+\/>'
IMAGE_REG_2 = r'^!\[\]\((\S+)\)'
LIMIT = 1000
TEXT_COLOR = '#FFFFFF'
DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
FIRST_COLUMN_TYPES = [
    ColumnTypes.TEXT, ColumnTypes.NUMBER, ColumnTypes.DATE, ColumnTypes.SINGLE_SELECT, ColumnTypes.AUTO_NUMBER]
AIRTABLE_API_URL = 'https://api.airtable.com/v0/'
FILE = 'file'
IMAGE = 'image'

logger = logging.getLogger(__name__)


class LinksConvertor(object):
    def convert(self, column_name, link_data, airtable_rows):
        links = self.gen_links(column_name, link_data, airtable_rows)
        return links

    def gen_links(self, column_name, link_data, airtable_rows):
        row_id_list = []
        other_rows_ids_map = {}
        try:
            for row in airtable_rows:
                row_id = row['_id']
                link = row.get(column_name)
                if not link:
                    continue
                row_id_list.append(row_id)
                other_rows_ids_map[row_id] = link
        except Exception as e:
            logger.exception('Error during link generation')
        links = {
            'link_id': link_data['link_id'],
            'table_id': link_data['table_id'],
            'other_table_id': link_data['other_table_id'],
            'row_id_list': row_id_list,
            'other_rows_ids_map': other_rows_ids_map,
        }
        return links


class FilesConvertor(object):
    def __init__(self, airtable_access_token, base):
        self.airtable_api_headers = {
            'Authorization': 'Bearer ' + airtable_access_token}
        self.base = base

    def upload_file(self, item, file_type):
        try:
            # download from airtable
            name = item['filename']
            url = item['url']
            response = requests.get(
                url=url, headers=self.airtable_api_headers, timeout=60)
            content = response.content
            # upload to seatable
            file_info = self.base.upload_bytes_file(
                name=name, content=content, file_type=file_type)
            return file_info
        except Exception as e:
            logger.exception('Could not upload file')
            return None

    def batch_upload_files(self, value):
        file_list = []
        for item in value:
            file_info = self.upload_file(item, file_type=FILE)
            if file_info is not None:
                file_list.append(file_info)
        return file_list

    def batch_upload_images(self, value):
        image_list = []
        for item in value:
            file_info = self.upload_file(item, file_type=IMAGE)
            if file_info is not None:
                image_url = file_info['url']
                image_list.append(image_url)
        return image_list


class RowsConvertor(object):
    def __init__(self, files_convertor):
        self.files_convertor = files_convertor

    def convert(self, columns, airtable_rows):
        rows = self.gen_rows(columns, airtable_rows)
        return rows

    def parse_date(self, value):
        value = str(value)
        if len(value) == 10:
            value = str(datetime.strptime(value, DATE_FORMAT))
        elif len(value) == 24:
            value = str(datetime.strptime(value, DATETIME_FORMAT))
        return value

    def parse_image(self, value):
        image_list = self.files_convertor.batch_upload_images(value)
        return image_list

    def parse_file(self, value):
        file_list = self.files_convertor.batch_upload_files(value)
        return file_list

    def parse_single_select(self, value):
        if isinstance(value, dict) and 'name' in value:
            return value['name']
        value = str(value)
        return value

    def parse_multiple_select(self, value):
        if isinstance(value[0], dict):
            collaborators = []
            for item in value:
                collaborators.append(item['name'])
            return collaborators
        else:
            return [str(item) for item in value]

    def parse_text(self, value):
        if isinstance(value, dict) and 'text' in value:
            return value['text']
        if isinstance(value, dict) and 'name' in value:
            return value['name']
        if isinstance(value, list) and value and \
                isinstance(value[0], dict) and 'name' in value[0]:
            collaborators = []
            for item in value:
                collaborators.append(item['name'])
            return ', '.join(collaborators)
        value = str(value)
        return value

    def parse_long_text(self, value):
        value = str(value)
        checked_count = value.count('[x]')
        unchecked_count = value.count('[ ]')
        total = checked_count + unchecked_count
        href_reg = re.compile(HREF_REG)
        preview = href_reg.sub(' ', value)
        preview = preview[:20].replace('\n', ' ')
        images = []
        links = []
        href_list = href_reg.findall(value)
        for href in href_list:
            if re.search(LINK_REG_1, href):
                links.append(re.search(LINK_REG_1, href).group(1))
            elif re.search(LINK_REG_2, href):
                links.append(re.search(LINK_REG_2, href).group(1))
            elif re.search(IMAGE_REG_1, href):
                images.append(re.search(IMAGE_REG_1, href).group(1))
            elif re.search(IMAGE_REG_2, href):
                images.append(re.search(IMAGE_REG_2, href).group(1))
        return {
            'text': value,
            'preview': preview,
            'checklist': {'completed': checked_count, 'total': total},
            'images': images,
            'links': links,
        }

    def gen_cell_data(self, column_type, value):
        try:
            if column_type == ColumnTypes.CHECKBOX:
                cell_data = value
            elif column_type in (ColumnTypes.NUMBER, ColumnTypes.DURATION, ColumnTypes.RATE):
                cell_data = value
            elif column_type in (ColumnTypes.URL, ColumnTypes.EMAIL):
                cell_data = str(value)
            elif column_type == ColumnTypes.TEXT:
                cell_data = self.parse_text(value)
            elif column_type == ColumnTypes.LONG_TEXT:
                cell_data = self.parse_long_text(value)
            elif column_type == ColumnTypes.DATE:
                cell_data = self.parse_date(value)
            elif column_type == ColumnTypes.FILE:
                cell_data = self.parse_file(value)
            elif column_type == ColumnTypes.IMAGE:
                cell_data = self.parse_image(value)
            elif column_type == ColumnTypes.SINGLE_SELECT:
                cell_data = self.parse_single_select(value)
            elif column_type == ColumnTypes.MULTIPLE_SELECT:
                cell_data = self.parse_multiple_select(value)
            elif column_type == ColumnTypes.LINK:
                cell_data = None
            elif column_type == ColumnTypes.BUTTON:
                cell_data = None
            elif column_type == ColumnTypes.GEOLOCATION:
                cell_data = None
            elif column_type in (ColumnTypes.COLLABORATOR, ColumnTypes.CREATOR, ColumnTypes.LAST_MODIFIER):
                cell_data = None
            elif column_type in (ColumnTypes.CTIME, ColumnTypes.MTIME):
                cell_data = None
            elif column_type in (ColumnTypes.FORMULA, ColumnTypes.LINK_FORMULA, ColumnTypes.AUTO_NUMBER):
                cell_data = None
            else:
                cell_data = str(value)
        except Exception as e:
            logger.exception('Could not generate cell data')
            cell_data = str(value)
        return cell_data

    def gen_rows(self, columns, airtable_rows):
        rows = []
        for row in airtable_rows:
            row_data = {'_id': row['_id']}
            for column in columns:
                column_name = column['name']
                column_type = column['type']

                value = row.get(column_name)
                if value is None:
                    continue
                cell_data = self.gen_cell_data(column_type, value)
                row_data[column_name] = cell_data
            if row_data:
                rows.append(row_data)
        return rows


class ColumnsParser(object):
    def parse(self, link_map, table_name, airtable_rows):
        value_map = self.get_value_map(airtable_rows)
        columns = self.gen_columns(link_map, table_name, value_map)
        return columns

    def random_color(self):
        color_str = '0123456789ABCDEF'
        color = '#'
        for i in range(6):
            color += random.choice(color_str)
        return color

    def random_num_id(self):
        num_str = '0123456789'
        num = ''
        for i in range(6):
            num += random.choice(num_str)
        return num

    def get_value_map(self, airtable_rows):
        value_map = {}
        for row in airtable_rows:
            for column_name, value in row.items():
                if column_name not in value_map:
                    value_map[column_name] = []
                if value is not None:
                    value_map[column_name].append(value)
        return value_map

    def get_select_options(self, select_list):
        return [{
            'name': str(value),
            'id': self.random_num_id(),
            'color': self.random_color(),
            'textColor': TEXT_COLOR,
        } for value in select_list]

    def get_column_data(self, link_map, table_name, column_name, column_type, values):
        column_data = None
        try:
            if column_type == 'barcode':
                column_type = ColumnTypes.TEXT
            elif column_type == ColumnTypes.DATE:
                column_data = {'format': 'YYYY-MM-DD'}
            elif column_type == ColumnTypes.LINK:
                link_info = link_map.get(table_name, {}).get(column_name)
                other_table_name = link_info.get('other_table') if link_info else None
                if other_table_name:
                    column_data = {'other_table': other_table_name}
                else:
                    column_type = ColumnTypes.TEXT
            elif column_type == ColumnTypes.MULTIPLE_SELECT:
                select_list = []
                for value in values:
                    for item in value:
                        item = str(item)
                        if item not in select_list:
                            select_list.append(item)
                column_data = {'options': self.get_select_options(select_list)}
            elif column_type == ColumnTypes.COLLABORATOR:
                column_type = ColumnTypes.TEXT
        except Exception as e:
            logger.warning('get %s column %s data error: %s', column_type, column_name, e)
        return column_type, column_data

    def get_column_type(self, values):
        column_type = ColumnTypes.TEXT
        try:
            type_list = []
            for value in values:
                # bool
                if value is True:
                    column_type = ColumnTypes.CHECKBOX
                # number
                elif isinstance(value, int) or isinstance(value, float):
                    column_type = ColumnTypes.NUMBER
                # str
                elif isinstance(value, str):
                    if '-' in value and len(value) == 10:
                        try:
                            datetime.strptime(value, DATE_FORMAT)
                            column_type = ColumnTypes.DATE
                        except ValueError:
                            pass
                    elif '-' in value and len(value) == 24:
                        try:
                            datetime.strptime(value, DATETIME_FORMAT)
                            column_type = ColumnTypes.DATE
                        except ValueError:
                            pass
                    elif value.startswith('http://') or value.startswith('https://'):
                        column_type = ColumnTypes.URL
                    elif '@' in value and '.' in value:
                        column_type = ColumnTypes.EMAIL
                    elif '\n' in value or len(value) > 50:
                        column_type = ColumnTypes.LONG_TEXT
                # list
                elif isinstance(value, list):
                    if not value:
                        continue
                    elif isinstance(value[0], str) and value[0].startswith('rec'):
                        column_type = ColumnTypes.LINK
                    elif isinstance(value[0], dict):
                        if 'email' in value[0]:
                            column_type = ColumnTypes.COLLABORATOR
                        elif 'filename' in value[0]:
                            column_type = ColumnTypes.FILE
                    else:  # str, number, boole
                        column_type = ColumnTypes.MULTIPLE_SELECT
                elif isinstance(value, dict):
                    if not value:
                        continue
                    elif 'email' in value:
                        column_type = ColumnTypes.COLLABORATOR
                    elif 'label' in value:
                        column_type = ColumnTypes.BUTTON
                    elif 'text' in value:
                        column_type = 'barcode'
                type_list.append(column_type)
            if not type_list:
                column_type = ColumnTypes.TEXT
            elif ColumnTypes.LONG_TEXT in type_list:
                column_type = ColumnTypes.LONG_TEXT
            else:
                column_type = max(type_list, key=type_list.count)
        except Exception as e:
            logger.warning('get column type error: %s', e)
        return column_type

    def gen_columns(self, link_map, table_name, value_map):
        columns = []
        for column_name, values in value_map.items():
            if column_name == '_id':
                continue
            column_type = self.get_column_type(values)
            column_type, column_data = self.get_column_data(
                link_map, table_name, column_name, column_type, values)
            if column_type == ColumnTypes.LINK and not column_data:
                continue
            column = {
                'name': column_name,
                'type': column_type,
                'data': column_data,
            }
            columns.append(column)
        return columns


class AirtableAPI(object):
    def __init__(self, airtable_access_token, airtable_base_id):
        self.airtable_access_token = airtable_access_token
        self.airtable_base_id = airtable_base_id

    def __str__(self):
        return '<Airtable API [ %s ]>' % (self.airtable_base_id)

    def list_rows(self, table_name, offset=''):
        headers = {'Authorization': 'Bearer ' + self.airtable_access_token}
        url = AIRTABLE_API_URL + self.airtable_base_id + '/' + urllib.parse.quote(table_name, safe='')
        if offset:
            url = url + '?offset=' + offset
        response = requests.get(url, headers=headers, timeout=60)
        if response.status_code >= 400:
            raise ConnectionError(response.status_code, response.text)
        response_dict = response.json()
        offset = response_dict.get('offset')
        records = response_dict['records']
        rows = []
        for record in records:
            row = record['fields']
            row['_id'] = record['id']
            rows.append(row)
        return rows, offset

    def list_all_rows(self, table_name):
        all_rows = []
        offset = ''
        while True:
            rows, offset = self.list_rows(table_name, offset)
            all_rows.extend(rows)
            logger.info('Retrieved %d rows from table "%s"', len(all_rows), table_name)
            if not offset:
                break
        return all_rows

    def get_schema(self):
        url = f'{AIRTABLE_API_URL}meta/bases/{self.airtable_base_id}/tables'
        headers = {'Authorization': 'Bearer ' + self.airtable_access_token}

        response = requests.get(url, headers=headers, timeout=60)
        if response.status_code >= 400:
            raise ConnectionError(response.status_code, response.text)

        return response.json().get('tables', [])


class AirtableConvertor(object):
    def __init__(self, airtable_access_token, airtable_base_id, base, excluded_column_types=[], excluded_columns=[]):
        self.airtable_api = AirtableAPI(airtable_access_token, airtable_base_id)
        self.base = base
        self.excluded_column_types = excluded_column_types
        self.excluded_columns = excluded_columns
        self.manually_migrated_columns = []
        self.columns_parser = ColumnsParser()
        self.files_convertor = FilesConvertor(airtable_access_token, base)
        self.rows_convertor = RowsConvertor(self.files_convertor)
        self.links_convertor = LinksConvertor()
        self.table_names = []
        self.first_column_map = {}
        self.link_map = {}

    def convert_airtable_to_seatable(self):        
        schema = self.airtable_api.get_schema()
        self.extract_schema_info(schema)
        self.parse_airtable_schema(schema)
        self.get_airtable_row_map()
        
        # Create tables and columns
        self.convert_tables()
        self.add_helper_table()
        self.convert_columns()
        
        self.convert_rows()
        self.convert_links()
        

    def parse_airtable_schema(self, schema):
        self.airtable_column_map = {}

        COLUMN_MAPPING = {
            # From https://airtable.com/developers/web/api/model/field-type
            # Note: Commented out column types are not supported and must be manually created
            "singleLineText": ColumnTypes.TEXT,
            "email": ColumnTypes.EMAIL,
            "url": ColumnTypes.URL,
            "multilineText": ColumnTypes.LONG_TEXT,
            "number": ColumnTypes.NUMBER,
            "percent": ColumnTypes.NUMBER,
            "currency": ColumnTypes.NUMBER,
            "singleSelect": ColumnTypes.SINGLE_SELECT,
            "multipleSelects": ColumnTypes.MULTIPLE_SELECT,
            "singleCollaborator": ColumnTypes.TEXT,
            "multipleCollaborators": ColumnTypes.TEXT,
            "multipleRecordLinks": ColumnTypes.LINK,
            "date": ColumnTypes.DATE,
            "dateTime": ColumnTypes.DATE,
            # SeaTable does not support phone number columns
            "phoneNumber": ColumnTypes.TEXT,
            "multipleAttachments": ColumnTypes.FILE,
            "checkbox": ColumnTypes.CHECKBOX,
            "formula": ColumnTypes.FORMULA,
            "createdTime": ColumnTypes.DATE,
            #"rollup": '',
            #"count": '',
            #"lookup": '',
            #"multipleLookupValues": '',
            "autoNumber": ColumnTypes.TEXT,
            # SeaTable does not support barcode columns
            "barcode": ColumnTypes.TEXT,
            "rating": ColumnTypes.RATE,
            "richText": ColumnTypes.LONG_TEXT,
            "duration": ColumnTypes.DURATION,
            "lastModifiedTime": ColumnTypes.DATE,
            # "button": ColumnTypes.BUTTON,
            "createdBy": ColumnTypes.TEXT,
            "lastModifiedBy": ColumnTypes.TEXT,
            #"externalSyncSource": '',
            #"aiText": '',
        }


        for table in schema:
            columns = []

            for field in table['fields']:
                column_name = field['name']
                column_type = field['type']

                if column_name == '_id':
                    continue

                seatable_column_type = COLUMN_MAPPING.get(column_type)

                if seatable_column_type is None:
                    logger.warning('Column "%s" (table "%s") is of type "%s"; column must be manually added', column_name, table['name'], column_type)
                    self.manually_migrated_columns.append({'Column': column_name, 'Table': table['name'], 'Type': column_type, 'Metadata': json.dumps(field.get('options', ''))})
                    continue

                if seatable_column_type == ColumnTypes.DATE:
                    column_data = {'format': 'YYYY-MM-DD HH:mm'}
                elif seatable_column_type == ColumnTypes.NUMBER:
                    if column_type == 'number':
                        column_data = {'format': 'number', 'decimal': 'dot', 'thousands': 'no'}
                    elif column_type == 'percent':
                        column_data = {'format': 'percent', 'decimal': 'dot', 'thousands': 'no'}
                    elif column_type == 'currency':
                        column_data = {'format': 'dollar', 'decimal': 'dot', 'thousands': 'no'}
                    else:
                        column_data = {}
                elif seatable_column_type == ColumnTypes.LINK:
                    link_info = self.link_map.get(table['name'], {}).get(column_name)

                    if not link_info:
                        logger.warning('Column "%s" (table "%s") was not found in link map', column_name, table['name'])
                        continue

                    column_data = {'other_table': link_info.get('other_table')}
                elif seatable_column_type in [ColumnTypes.SINGLE_SELECT, ColumnTypes.MULTIPLE_SELECT]:
                    column_data = {
                        'options': self.get_select_options(field['options']['choices']),
                    }
                elif seatable_column_type == ColumnTypes.DURATION:
                    column_data = {
                        'format': 'duration',
                        'duration_format': 'h:mm:ss',
                    }
                elif seatable_column_type == ColumnTypes.FORMULA:
                    column_data = {'formula': '"Formula to be defined"'}
                elif seatable_column_type == ColumnTypes.RATE:
                    column_data = {'rate_max_number': field['options']['max']}
                else:
                    column_data = {}

                column = {
                    'name': column_name,
                    'type': seatable_column_type,
                    'data': column_data,
                }

                columns.append(column)

            self.airtable_column_map[table['name']] = columns

    def get_select_options(self, options):
        return [{
            'name': value['name'],
            'id': self.random_num_id(),
            'color': self.random_color(),
            'textColor': TEXT_COLOR,
        } for value in options]

    def random_num_id(self):
        num_str = '0123456789'
        num = ''
        for i in range(6):
            num += random.choice(num_str)
        return num

    def random_color(self):
        color_str = '0123456789ABCDEF'
        color = '#'
        for i in range(6):
            color += random.choice(color_str)
        return color

    def convert_tables(self):
        logger.info('Start adding tables and columns in SeaTable base')
        self.get_table_map()
        for table_name in self.table_names:
            table = self.table_map.get(table_name)
            if not table:
                airtable_columns = self.airtable_column_map[table_name]
                first_column_name = self.first_column_map.get(table_name) or \
                    airtable_columns[0]['name']
                columns = []
                for column in airtable_columns:
                    if column['type'] == ColumnTypes.LINK:
                        # Skip link columns for now
                        continue
                    item = {
                        'column_name': column['name'],
                        'column_type': column['type'],
                        'column_data': column['data'],
                    }
                    if column['name'] == first_column_name:
                        if column['type'] not in FIRST_COLUMN_TYPES:
                            item['column_type'] = ColumnTypes.TEXT
                            item['column_data'] = None
                        columns.insert(0, item)
                    else:
                        columns.append(item)
                self.add_table(table_name, columns)
                logger.info('Added table "%s" with %d columns', table_name, len(columns))
        logger.info('Tables and columns added in SeaTable base')

    def add_helper_table(self):
        table_name = 'Columns to be migrated manually'

        columns = [
            {'column_name': 'Column', 'column_type': ColumnTypes.TEXT},
            {'column_name': 'Table', 'column_type': ColumnTypes.TEXT},
            {'column_name': 'Type', 'column_type': ColumnTypes.TEXT},
            {'column_name': 'Metadata', 'column_type': ColumnTypes.LONG_TEXT},
            {'column_name': 'Completed', 'column_type': ColumnTypes.CHECKBOX},
        ]

        self.add_table(table_name, columns)
        logger.info('Table "%s" added', table_name)

        self.batch_append_rows(table_name, self.manually_migrated_columns)

    def convert_columns(self):
        logger.info('Start adding link columns in SeaTable base')
        self.get_table_map()
        for table_name in self.table_names:
            airtable_columns = self.airtable_column_map[table_name]
            table = self.table_map.get(table_name)
            for column in airtable_columns:
                column_name = column['name']
                exists_column = table.get(column_name)
                if not exists_column:
                    new_column = self.add_column(
                        table_name, column_name, column['type'], column['data'])
                    if column['type'] == ColumnTypes.LINK:
                        link_info = self.link_map.get(table_name, {}).get(column_name)
                        self.sync_linked_column_name(table_name, column_name, link_info)
                    logger.info('Added column "%s" to table "%s"', column['name'], table_name)
        logger.info('Link columns added in SeaTable base')

    def convert_rows(self):
        """Import all rows"""
        logger.info('Start appending rows in SeaTable base')
        self.get_table_map()
        for table_name in self.table_names:
            airtable_rows = self.airtable_row_map[table_name]
            columns = self.column_map[table_name]

            # Remove excluded column types
            columns = [c for c in columns if c['type'] not in self.excluded_column_types]

            # Remove excluded columns
            columns = [
                column for column in columns
                if (table_name, column['name']) not in self.excluded_columns
            ]

            rows = self.rows_convertor.convert(columns, airtable_rows)
            self.batch_append_rows(table_name, rows)
        logger.info('Rows appended in SeaTable base')

    def convert_links(self):
        """Import all links"""
        if not self.link_map:
            return
        logger.info('Start adding links between records in SeaTable base')
        self.get_table_map()
        for table_name, column_names in self.link_map.items():
            try:
                table = self.table_map[table_name]
                airtable_rows = self.airtable_row_map[table_name]
                for column_name in column_names:
                    try:
                        link_data = table[column_name]['data']
                        links = self.links_convertor.convert(
                            column_name, link_data, airtable_rows)
                        self.batch_append_links(table_name, links)
                    except Exception as e:
                        logger.error('Failed to process links for column "%s" in table "%s": %s', column_name, table_name, e)
                        continue
            except Exception as e:
                logger.error('Failed to process links for table "%s": %s', table_name, e)
                continue
        logger.info('Links added between records in SeaTable base')

    def extract_schema_info(self, schema):
        """Auto-extract table_names and link_map from schema"""
        self.table_names = [table['name'] for table in schema]
        table_id_map = {table['id']: table for table in schema}
        for table in schema:
            table_name = table['name']
            primary_field_id = table['primaryFieldId']
            
            primary_field = next(
                (field for field in table['fields'] if field['id'] == primary_field_id),
                None
            )
            
            if primary_field:
                self.first_column_map[table_name] = primary_field['name']
        
        processed_field_pairs = set()

        def add_link_mapping(src_table_name, src_field, dst_table_name, dst_field_name, dst_table):
            if src_table_name not in self.link_map:
                self.link_map[src_table_name] = {}
            self.link_map[src_table_name][src_field['name']] = {
                'other_table': dst_table_name,
                'linked_field_name': dst_field_name,
                'linked_table_id': dst_table.get('id') if dst_table else None,
            }

        for table in schema:
            table_name = table['name']
            
            for field in table['fields']:
                if field['type'] == 'multipleRecordLinks':
                    linked_table_id = field['options']['linkedTableId']
                    linked_table = table_id_map.get(linked_table_id)
                    if not linked_table:
                        continue

                    linked_table_name = linked_table['name']
                    current_field_id = field['id']
                    inverse_field_id = field['options'].get('inverseLinkFieldId')

                    field_pair_key = None
                    linked_field = None
                    if inverse_field_id:
                        linked_field = next(
                            (f for f in linked_table['fields'] if f['id'] == inverse_field_id),
                            None
                        )
                        field_pair_key = tuple(sorted([current_field_id, inverse_field_id]))

                    if field_pair_key and field_pair_key in processed_field_pairs:
                        continue

                    if field_pair_key:
                        processed_field_pairs.add(field_pair_key)

                    linked_field_name = linked_field['name'] if linked_field else None

                    add_link_mapping(table_name, field, linked_table_name, linked_field_name, linked_table)

                    if linked_field:
                        add_link_mapping(linked_table_name, linked_field, table_name, field['name'], table)

    def get_airtable_row_map(self):
        """Get all data from Airtable"""
        logger.info('Start retrieving data from Airtable')

        self.airtable_row_map = {}
        for table_name in self.table_names:
            rows = self.airtable_api.list_all_rows(table_name)
            self.airtable_row_map[table_name] = rows

        logger.info('Data retrieved from Airtable')
        return self.airtable_row_map

    def get_table_map(self):
        self.table_map = {}
        self.column_map = {}
        metadata = self.base.get_metadata()
        self.tables = metadata['tables']
        for table in self.tables:
            table_name = table['name']
            self.column_map[table_name] = table['columns']
            self.table_map[table_name] = {'_id': table['_id']}
            for column in table['columns']:
                column_name = column['name']
                self.table_map[table_name][column_name] = column
        return self.table_map

    def add_table(self, table_name, columns):
        table = self.base.add_table(table_name, columns=columns)
        return table

    def add_column(self, table_name, column_name, column_type, column_data):
        try:
            column = self.base.insert_column(
                table_name, column_name, column_type, column_data)
            return column
        except Exception as e:
            logger.error('add column error: %s', e)

    def sync_linked_column_name(self, table_name, column_name, link_info):
        if not link_info:
            return

        linked_field_name = link_info.get('linked_field_name')
        linked_table_name = link_info.get('other_table')

        if not linked_field_name or not linked_table_name:
            return

        try:
            source_columns = self.base.list_columns(table_name)
        except Exception as e:
            logger.warning('Failed to list columns for table "%s" when syncing linked column "%s": %s', table_name, column_name, e)
            return

        source_column = next((col for col in source_columns if col.get('name') == column_name), None)
        if not source_column:
            logger.warning('Could not find inserted link column "%s" in table "%s" while syncing names', column_name, table_name)
            return

        link_data = source_column.get('data') or {}
        link_id = link_data.get('link_id')
        if not link_id:
            logger.warning('Link column "%s" in table "%s" missing link_id, skip renaming linked column', column_name, table_name)
            return

        try:
            linked_columns = self.base.list_columns(linked_table_name)
        except Exception as e:
            logger.warning('Failed to list columns for linked table "%s" when syncing link column "%s": %s', linked_table_name, column_name, e)
            return

        target_column = None
        for col in linked_columns:
            data = col.get('data') or {}
            if data.get('link_id') == link_id:
                target_column = col
                break

        if not target_column:
            logger.warning('Linked column for link_id "%s" not found in table "%s"', link_id, linked_table_name)
            return

        if target_column.get('name') == linked_field_name:
            return

        try:
            self.base.rename_column(linked_table_name, target_column['key'], linked_field_name)
        except Exception as e:
            logger.warning('Failed to rename linked column in table "%s" to "%s": %s', linked_table_name, linked_field_name, e)

    def list_rows(self, table_name):
        rows = self.base.list_rows(table_name)
        return rows

    def batch_append_rows(self, table_name, rows):
        offset = 0
        while True:
            row_split = rows[offset: offset + LIMIT]
            offset = offset + LIMIT
            if not row_split:
                break
            self.base.batch_append_rows(table_name, row_split)
            logger.info('Appended %d rows to table "%s"', len(row_split), table_name)

    def batch_append_links(self, table_name, links):
        link_id = links['link_id']
        table_id = links['table_id']
        other_table_id = links['other_table_id']
        row_id_list = links['row_id_list']
        other_rows_ids_map = links['other_rows_ids_map']
        
        if not row_id_list:
            return
            
        offset = 0
        while True:
            row_id_split = row_id_list[offset: offset + LIMIT]
            offset = offset + LIMIT
            if not row_id_split:
                break
            other_rows_ids_map_split = {
                row_id: other_rows_ids_map[row_id] for row_id in row_id_split}
            try:
                self.base.batch_update_links(
                    link_id, table_id, other_table_id, row_id_split, other_rows_ids_map_split)
            except Exception as e:
                logger.warning('Failed to add %d links to table "%s": %s', len(row_id_split), table_name, e)
                continue
