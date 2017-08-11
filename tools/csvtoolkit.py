
import csv
import itertools
import json
import os
import pprint

class CSVException(Exception): pass

class Row(object):
	def __init__(self, data, fieldNames=None, lineNumber=None, filename=None):
		self.data = data
		self.fieldNames = fieldNames
		self.lineNumber = lineNumber
		self.filename = filename
		if self.fieldNames is None:
			self.fieldNames = sorted(self.data.keys())
	def clone(self):
		return Row(self.data, self.fieldNames, self.lineNumber, self.filename)
	def __repr__(self):
		return repr(self.__dict__)

#================================================================================
# HELPERS

def _findDelimiter(f):
	"""Given an already-open file object, find the likely CSV delimiter.
	Afterwords, seek the file back to the beginning.
	"""
	line = f.readline()
	for char in '\t,':
		if char in line:
			f.seek(0)
			return char
	return ','

def _tryEncode(s, encoding='utf8'):
	if isinstance(s, unicode):
		return s.encode(encoding)
	else:
		return s
def _tryDecode(s, encoding='utf8'):
	if isinstance(s, str):
		return s.decode(encoding)
	else:
		return s

def _encodeDict(d, encoding='utf8'):
	return dict((_tryEncode(k, encoding), _tryEncode(v, encoding)) for (k, v) in d.items())
def _decodeDict(d, encoding='utf8'):
	return dict((_tryDecode(k, encoding), _tryDecode(v, encoding)) for (k, v) in d.items())

def _encodeList(lst, encoding='utf8'):
	return [_tryEncode(i) for i in lst]
def _decodeList(lst, encoding='utf8'):
	return [_tryDecode(i) for i in lst]

#================================================================================
# INPUT AND OUTPUT

def getRowStream(fn, encoding='utf8'):
	"""Yield Row objects from a CSV file."""
	with file(fn, 'rU') as f:
		reader = csv.DictReader(f, delimiter=_findDelimiter(f))
		for ii, row in enumerate(reader):
			data = _decodeDict(row, encoding)
			yield Row(data=data, fieldNames=reader.fieldnames, lineNumber=ii+2, filename=fn)

def writeRows(rowStream, outputFn):
	"""Write Rows to a CSV file."""
	with file(outputFn, 'w') as f:
		for ii, row in enumerate(rowStream):
			if ii == 0:
				writer = csv.DictWriter(f, fieldnames=row.fieldNames)
				writer.writeheader()
			writer.writerow(_encodeDict(row.data))
			yield row

def writeRowsMultiPartAndDiscoverColumns(rowStream, outputFn, numPerFile=1000):
	buffer = []
	fileNum = 0
	extraRow = None
	while True:
		# for each chunk, create a smaller stream and run it
		outputFnPart = outputFn.replace('.csv', '.part-{:05n}.csv'.format(fileNum))
		r = itertools.islice(rowStream, numPerFile)
		r = discoverColumns(r)
		r = writeRows(r, outputFnPart)
		foundAny = False
		for row in r:
			foundAny = True
			yield row
		if not foundAny:
			os.unlink(outputFnPart)
			return
		fileNum += 1

# def writeRowsMultiPartOld(rowStream, outputFn, numPerFile=1000):
# 	"""Write Rows to a CSV file."""
# 	fileNum = 0
# 	while True:
# 		outputFnPart = outputFn.replace('.csv', '.part-{:05n}.csv'.format(fileNum))
# 		with file(outputFnPart, 'w') as f:
# 			wroteAny = False
# 			for ii, row in enumerate(rowStream):
# 				wroteAny = True
# 				if ii == 0:
# 					writer = csv.DictWriter(f, fieldnames=row.fieldNames)
# 					writer.writeheader()
# 				writer.writerow(_encodeDict(row.data))
# 				yield row
# 				if ii == numPerFile - 1: break
# 		if not wroteAny:
# 			os.unlink(outputFnPart)
# 			return
# 		fileNum += 1

def runStream(stream):
	count = 0
	for item in stream:
		count += 1
	return count

def show(stream):
	for item in stream:
		if isinstance(item, Row):
			print('line {}: {}'.format(item.lineNumber, item.data))
		else:
			print(item)
		yield item

def showJson(stream, divider=None):
	for item in stream:
		if divider: print(divider)
		if isinstance(item, Row):
			print('line {}: {}'.format(item.lineNumber, json.dumps(item.data, sort_keys=True, indent=2)))
		else:
			print(json.dumps(item.data, sort_keys=True, indent=2))
		yield item

#================================================================================
# TRANSFORM / FILTER

def mapFn(stream, fn, *args, **kwargs):
	for item in stream:
		if isinstance(item, Row):
			item = item.clone()
		result = fn(item, *args, **kwargs)
		if isinstance(result, Row) and isinstance(item, Row):
			result.lineNumber = item.lineNumber
		yield result

def filterFn(stream, fn, *args, **kwargs):
	for item in stream:
		if fn(item, *args, **kwargs):
			yield item

#================================================================================
# MANIPULATE COLUMNS

def discoverColumns(rowStream):
	"""Read the entire stream, then go back and set fieldNames on each row.
	"""
	rows = list(rowStream)
	fieldNames = set()
	for row in rows:
		fieldNames.update(row.data.keys())
	fieldNames = sorted(list(fieldNames))
	rows.reverse()
	while rows:
		thisRow = rows.pop()
		thisRow.fieldNames = fieldNames
		yield thisRow


def renameColumn(rowStream, oldName, newName):
	for row in rowStream:
		row2 = row.clone()
		if oldName in row2.data:
			row2.data[newName] = row2.data[oldName]
			del row2.data[oldName]
		row2.fieldNames = [(newName if fn == oldName else fn) for fn in row2.fieldNames]
		yield row2

def renameColumns(rowStream, nameRemappingDict):
	"""nameRemappingDict should be in the form {oldName: newName, ...}
	"""
	for row in rowStream:
		row2 = row.clone()
		data2 = {}
		for k, v in row2.data.items():
			data2[nameRemappingDict.get(k, k)] = v
		row2.data = data2
		row2.fieldNames = [nameRemappingDict.get(fn, fn) for fn in row2.fieldNames]
		yield row2

def moveColumnToLeft(rowStream, fieldName):
	for row in rowStream:
		row2 = row.clone()
		if fieldName in row2.fieldNames:
			row2.fieldNames.remove(fieldName)
			row2.fieldNames = [fieldName] + row2.fieldNames
		yield row2

def moveColumnToRight(rowStream, fieldName):
	for row in rowStream:
		row2 = row.clone()
		if fieldName in row2.fieldNames:
			row2.fieldNames.remove(fieldName)
			row2.fieldNames.append(fieldName)
		yield row2

def setColumn(rowStream, fieldName, value):
	for row in rowStream:
		row2 = row.clone()
		if fieldName not in row2.fieldNames:
			row2.fieldNames.append(fieldName)
		row2.data[fieldName] = value
		yield row2

def setColumns(rowStream, fieldNamesAndValues):
	for row in rowStream:
		row2 = row.clone()
		for fieldName, value in fieldNamesAndValues.items():
			if fieldName not in row2.fieldNames:
				row2.fieldNames.append(fieldName)
			row2.data[fieldName] = value
		yield row2

def removeColumn(rowStream, fieldName):
	for row in rowStream:
		row2 = row.clone()
		if fieldName in row2.fieldNames:
			del row2.data[fieldName]
			row2.fieldNames.remove(fieldName)
		yield row2

def removeColumns(rowStream, fieldNames):
	for row in rowStream:
		row2 = row.clone()
		for fieldName in fieldNames:
			if fieldName in row2.fieldNames:
				del row2.data[fieldName]
				row2.fieldNames.remove(fieldName)
		yield row2

def onlyKeepColumns(rowStream, fieldNames):
	for row in rowStream:
		row2 = row.clone()
		row2.fieldNames = [fn for fn in row2.fieldNames if fn in fieldNames]
		for fn in row2.data.keys():
			if fn not in fieldNames:
				del row2.data[fn]
		yield row2

#================================================================================
# ASSERT

def requireColumn(rowStream, fieldName):
	for row in rowStream:
		if not fieldName in row.fieldNames or not fieldName in row.data:
			raise CSVException('{}: Missing column: "{}"'.format(row.filename, fieldName))
		yield row

def requireColumns(rowStream, fieldNames):
	for row in rowStream:
		for fieldName in fieldNames:
			if not fieldName in row.fieldNames or not fieldName in row.data:
				raise CSVException('{}: Missing column: "{}"'.format(row.filename, fieldName))
		yield row

def requireNotBlankIfColumnsExist(rowStream, fieldNames):
	for row in rowStream:
		for fn in fieldNames:
			if fn in row.data and row.data[fn] == '':
				raise CSVException('{}: line {}: column "{}" is blank but is not allowed to be blank.'.format(row.filename, row.lineNumber, fn))
		yield row

def requireUniqueValuesInColumn(rowStream, fieldName, ignoreEmptyCells=False):
	# if the column doesn't exist, that's also ok.
	valueToLineNumber = {}
	for row in rowStream:
		if fieldName in row.data:
			v = row.data[fieldName]
			if ignoreEmptyCells == True and v == '':
				pass
			else:
				if v in valueToLineNumber:
					raise CSVException('{}: line {}: column "{}" should contain unique values but the value "{}" is not unique; it was also seen on line {}'.format(row.filename, row.lineNumber, fieldName, v, valueToLineNumber[v]))
				valueToLineNumber[v] = row.lineNumber
		yield row

def requireUniqueValuesInColumns(rowStream, fieldNames, ignoreEmptyCells=False):
	for fn in fieldNames:
		rowStream = requireUniqueValuesInColumn(rowStream, fn, ignoreEmptyCells)
	for row in rowStream:
		yield row

#================================================================================
# REORDER

def sort(stream, key=None):
	rows = list(stream)
	rows.sort(key=key)
	rows.reverse()
	while rows:
		yield rows.pop()

def sortByColumn(rowStream, fieldName):
	rows = list(rowStream)
	rows.sort(key=lambda row: row.data[fieldName])
	rows.reverse()
	while rows:
		yield rows.pop()

def reverse(stream):
	items = list(stream)
	while items:
		yield items.pop()

#================================================================================
# TIMESTAMP HANDLING

def mturkTimestampToStandardTimestamp(mturkTime):
	"""Given a time in Mturk's format like "Mon Sep 14 21:09:21 PDT 2015",
	convert it to ISO 8601 format in UTC like "2015-09-15T04:09:21Z".
	On failure, return None.
	"""
	try:
		timeTuple = email.utils.parsedate_tz(mturkTime)
		unixTime = email.utils.mktime_tz(timeTuple)
		iso8601 = datetime.datetime.utcfromtimestamp(unixTime).isoformat() + 'Z'
		return iso8601
	except:
		return None

def testMturkTimestampToStandardTimestamp():
	assert mturkTimestampToStandardTimestamp('Mon Sep 14 21:09:21 PDT 2015') == '2015-09-15T04:09:21Z'
	assert mturkTimestampToStandardTimestamp('zzzzz') is None

def utcDatetimeToStandardTimestamp(dt):
	return dt.isoformat() + 'Z'

#================================================================================
# TESTS

if __name__ == '__main__':
	SNOWMAN = u'\u2603'

	fn = 'test-csv/fruits_excel.csv'
	encoding = 'mac-roman'

	# r = getRowStream(fn, encoding)
	# r = sortByColumn(r, 'color')
	# # r = sort(r, key=lambda row: row.data['color'])
	# r = reverse(r)
	# # r = requireNotBlankColumns(r, ['misc datatypes'])
	# r = show(r)
	# runStream(r)
	# import sys; sys.exit(0)

	testMturkTimestampToStandardTimestamp()

	#================================================================================
	r = getRowStream(fn, encoding)
	r = renameColumn(r, 'id', 'id2')
	r = setColumn(r, 'newColumn', 'newValue')
	r = removeColumn(r, 'kind')
	r = moveColumnToLeft(r, 'color')
	r = moveColumnToRight(r, 'name')

	def addSnowman(row):
		if row.data['name'] == 'cinnamon':
			row.data['misc datatypes'] = SNOWMAN
		return row
	r = mapFn(r, addSnowman)

	def isColor(row, color):
		return row.data['color'] == color
	r = filterFn(r, isColor, 'red')
	r = filterFn(r, lambda row: int(row.data['id2']) < 4)

	r = renameColumns(r, {
			'color': 'newColumn',
			'newColumn': 'color',
		})
	r = setColumns(r, {
			'a': 'aaa',
			'b': 'bbb',
			'id2': 'ididid',
		})
	# r = onlyKeepColumns(r, ['id2', 'color'])
	r = requireColumns(r, ['id2', 'color'])
	r = requireNotBlankIfColumnsExist(r, ['color'])
	r = sortByColumn(r, 'color')

	r = requireUniqueValuesInColumn(r, 'name')
	#r = requireUniqueValuesInColumn(r, 'newColumn')
	# r = requireUniqueValuesInColumns(r, ['name', 'misc datatypes'])

	r = show(r)
	r = writeRows(r, 'test-csv/fruits_test.csv')
	runStream(r)

