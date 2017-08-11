
import csvtoolkit as ct

# percepts must have these columns
PERCEPT_COLUMNS = {
	'p:byte_count': (),
	'p:credentials': ('not_null'),
	'p:device_id': (),
	'p:format': ('not_null'),
	'p:hash': ('not_null', 'unique'),
	'p:locator': ('not_null', 'unique'),
	'p:property:source': ('not_null'),
	'p:stamp': (), # iso6801 formatted timestamp in UTC ('Z' suffix)
	'p:tags': (),  # comma-separated string
	'p:x_size': (),
	'p:y_size': (),
}

# annotations must have these columns
ANNOTATION_COLUMNS = {
	'a:boundary': (),
	'a:confidence': (),
	'a:domain': ('not_null'),
	'a:percept_hash': (),
	'a:percept_locator': (),
	'a:model': (),
	'a:property:uid': ('unique'),
	'a:property:source': ('not_null'),
	'a:stamp': (), # iso6801 formatted timestamp in UTC ('Z' suffix)
	'a:tags': (),  # comma-separated string
}

def validateRows(rows):
	uniqueAnnotationColumns = [k for k,v in ANNOTATION_COLUMNS.items() if 'unique' in v]
	uniquePerceptColumns = [k for k,v in PERCEPT_COLUMNS.items() if 'unique' in v]
	rows = ct.requireUniqueValuesInColumns(rows, uniqueAnnotationColumns, ignoreEmptyCells=True)
	rows = ct.requireUniqueValuesInColumns(rows, uniquePerceptColumns, ignoreEmptyCells=True)
	rows = ct.mapFn(rows, _validateRow)
	return rows

def _validateRow(r):
	"""Raise an error if a bad row is found.
	For use in csvtoolkit.mapFn
	"""
	err = _validateRowDict(r.data)
	if err:
		raise ct.CSVException('{}: line {}: {}'.format(r.filename, r.lineNumber, err))
	return r

def _validateRowDict(d):
	"""Return None if good, or an error string if bad.
	"""
	# TODO: check that numeric columns contain valid numbers; likewise for JSON boundary field
	if not '_kind' in d:
		return '"_kind" column is required'
	if d['_kind'] == 'percept':
		for column, requirements in PERCEPT_COLUMNS.items():
			if not column in d:
				return '"{}" column is required but not present'.format(column)
			if 'not_null' in requirements and d[column] == '':
				return '"{}" column is blank but must not be blank'.format(column)
	elif d['_kind'] == 'annotation':
		for column, requirements in ANNOTATION_COLUMNS.items():
			if not column in d:
				return '"{}" column is required but not present'.format(column)
			if 'not_null' in requirements and d[column] == '':
				return '"{}" column is blank but must not be blank'.format(column)
		if bool(d['a:percept_hash']) == bool(d['a:percept_locator']):
			return 'must provide either "a:percept_hash" or "a:percept_locator", but got both (or neither)'
	else:
		return '"_kind" column must contain "percept" or "annotation", but "{}" was found'.format(d['_kind'])
	return None


