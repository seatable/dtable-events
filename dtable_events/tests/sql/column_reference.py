TEST_COLUMNS = [
    {'key': '0000', 'name': '名称', 'type': 'text', 'width': 126, 'editable': True, 'resizable': True},
    {'key': 'r1A1', 'type': 'date', 'name': 'Time2d', 'editable': True, 'width': 135, 'resizable': True, 'draggable': True, 'data': {'format': 'YYYY-MM-DD HH:mm'}, 'permission_type': '', 'permitted_users': [], 'editor': {'key': None, 'ref': None, 'props': {}, '_owner': None, '_store': {}}, 'formatter': {'key': None, 'ref': None, 'props': {}, '_owner': None, '_store': {}}},
    {'key': '_ctime', 'type': 'ctime', 'name': 'createTime', 'editable': True, 'width': 172, 'resizable': True, 'draggable': True, 'data': None, 'permission_type': '', 'permitted_users': []},
    {'key': 'PQ7r', 'type': 'single-select', 'name': 'Sing', 'editable': True, 'width': 117, 'resizable': True, 'draggable': True, 'data': {'options': [{'name': 'a', 'color': '#FFFCB5', 'textColor': '#202428', 'borderColor': '#E8E79D', 'id': '63347'}, {'name': 'b', 'color': '#F4667C', 'textColor': '#FFFFFF', 'borderColor': '#DC556A', 'id': '905189'}, {'name': 'c', 'color': '#9860E5', 'textColor': '#FFFFFF', 'borderColor': '#844BD2', 'id': '506341'}]}, 'permission_type': '', 'permitted_users': []},
    {'key': 'OhXJ', 'type': 'multiple-select', 'name': 'Mul', 'editable': True, 'width': 143, 'resizable': True, 'draggable': True, 'data': {'options': [{'name': 'aa', 'color': '#FFD9C8', 'textColor': '#202428', 'borderColor': '#EFBAA3', 'id': '885435'}, {'name': 'bb', 'color': '#EAA775', 'textColor': '#FFFFFF', 'borderColor': '#D59361', 'id': '764614'}, {'name': 'cc', 'color': '#9F8CF1', 'textColor': '#FFFFFF', 'borderColor': '#8F75E2', 'id': '418530'}, {'name': 'dd', 'color': '#ADDF84', 'textColor': '#FFFFFF', 'borderColor': '#9CCF72', 'id': '634546'}]}, 'permission_type': '', 'permitted_users': []},
    {'key': 'Dhi2', 'type': 'rate', 'name': 'rate', 'editable': True, 'width': 200, 'resizable': True, 'draggable': True, 'data': {'rate_max_number': 5, 'rate_style_color': '#FF8000'}, 'permission_type': '', 'permitted_users': []},
    {'key': 'W1lp', 'type': 'collaborator', 'name': 'Colla', 'editable': True, 'width': 200, 'resizable': True, 'draggable': True, 'data': {'enable_send_notification': False}, 'permission_type': '', 'permitted_users': []},
    {'key': '_creator', 'type': 'creator', 'name': 'Creator', 'editable': True, 'width': 200, 'resizable': True, 'draggable': True, 'data': None, 'permission_type': '', 'permitted_users': []},
    {'key': '_last_modifier', 'type': 'last-modifier', 'name': 'Modify', 'editable': True, 'width': 200, 'resizable': True, 'draggable': True, 'data': None, 'permission_type': '', 'permitted_users': []},
    {'key': '_mtime', 'type': 'mtime', 'name': 'modifyTime', 'editable': True, 'width': 200, 'resizable': True, 'draggable': True, 'data': None, 'permission_type': '', 'permitted_users': []},
    {'key': 'A47g', 'type': 'auto-number', 'name': 'AutoNo', 'editable': True, 'width': 200, 'resizable': True, 'draggable': True, 'data': {'format': '0000', 'max_used_auto_number': 906, 'digits': 4, 'prefix_type': None, 'prefix': None}, 'permission_type': '', 'permitted_users': []},
    {'key': 'G0yz', 'type': 'checkbox', 'name': 'CB', 'editable': True, 'width': 200, 'resizable': True, 'draggable': True, 'data': {'enable_fill_default_value': False, 'default_value': False}, 'permission_type': '', 'permitted_users': []},
    {'key': '6SJV', 'type': 'duration', 'name': 'Du', 'editable': True, 'width': 200, 'resizable': True, 'draggable': True, 'data': {'format': 'duration', 'duration_format': 'h:mm:ss'}, 'permission_type': '', 'permitted_users': []},
    {'key': '51r0', 'type': 'number', 'name': 'Num', 'editable': True, 'width': 200, 'resizable': True, 'draggable': True, 'data': {'format': 'number', 'precision': 2, 'enable_precision': False, 'enable_fill_default_value': False, 'decimal': 'dot', 'thousands': 'no'}, 'permission_type': '', 'permitted_users': []}
]

LINK_COLUMN = {"key":"G5J9","type":"link","name":"Link","editable":True,"width":200,"resizable":True,"draggable":True,"data":{"display_column_key":"0000","table_id":"0000","other_table_id":"kzvB","is_internal_link":True,"is_multiple":True,"is_row_from_view":False,"other_view_id":"","link_id":"55Sl","array_type":"text","array_data":None,"result_type":"array"},"permission_type":"","permitted_users":[],"edit_metadata_permission_type":"","edit_metadata_permitted_users":[],"description":None}

TABLES = [
    {
		"_id": "0000",
		"name": "Table1",
		"columns": [{
			"key": "G5J9",
			"type": "link",
			"name": "Link",
			"editable": True,
			"width": 200,
			"resizable": True,
			"draggable": True,
			"data": {
				"display_column_key": "0000",
				"table_id": "0000",
				"other_table_id": "kzvB",
				"is_internal_link": True,
				"is_multiple": True,
				"is_row_from_view": False,
				"other_view_id": "",
				"link_id": "55Sl",
				"array_type": "text",
				"array_data": None,
				"result_type": "array"
			},
		},],
	
	}, {
		"_id": "kzvB",
		"name": "Table2",
		"columns": [{
			"key": "3NIf",
			"type": "link",
			"name": "Table1",
			"editable": None,
			"width": 200,
			"resizable": None,
			"draggable": None,
			"data": {
				"display_column_key": "0000",
				"table_id": "0000",
				"other_table_id": "kzvB",
				"is_internal_link": None,
				"is_multiple": None,
				"is_row_from_view": None,
				"other_view_id": "",
				"link_id": "55Sl",
				"array_type": "text",
				"array_data": None,
				"result_type": "array"
			},
		}, ],
	}
]
