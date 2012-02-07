from rigor.config import config

import psycopg2
import psycopg2.extensions
import psycopg2.extras

from psycopg2.extensions import register_type
from psycopg2.extensions import register_adapter
from psycopg2.extensions import adapt
from psycopg2.extras import register_uuid
from psycopg2.pool import ThreadedConnectionPool

from contextlib import contextmanager

import ConfigParser

class RigorCursor(psycopg2.extras.DictCursor):
	""" Normal DictCursor with row mapping and enhanced fetch capabilities """

	def fetch_all(self, row_mapper=None):
		""" Fetches all rows, applying row mapper if any """
		if row_mapper:
			return [row_mapper.map_row(row) for row in self.fetchall()]
		else:
			return self.fetchall()

	def fetch_one(self, row_mapper=None):
		""" Fetches zero or one row, applying row mapper if any """
		row = self.fetchone()
		if row is None:
			return None
		if row_mapper is None:
			return row
		return row_mapper.map_row(row)

	def fetch_only_one(self, row_mapper=None):
		"""
		Fetches one and only one row.  Raises IntegrityError if there is not
		exactly one row found.
		"""
		if self.rowcount != 1:
			raise psycopg2.IntegrityError("Expected one record found, actually found %d. Query: %s" % (self.rowcount, self.query))
		return self.fetch_one(row_mapper)

class Database(object):
	""" Container for a database connection pool """

	def __init__(self):
		register_type(psycopg2.extensions.UNICODE)
		register_uuid()
		self._database_name = config.get('database', 'database')
		dsn = "dbname='{0}' host='{1}'".format(self._database_name, config.get('database', 'host'))
		try:
			ssl = config.getboolean('database', 'ssl')
			if ssl:
				dsn += " sslmode='require'"
		except ConfigParser.Error:
			pass
		try:
			username = config.get('database', 'username')
			dsn += " user='{0}'".format(username)
		except ConfigParser.Error:
			pass
		try:
			password = config.get('database', 'password')
			dsn += " password='{0}'".format(password)
		except ConfigParser.Error:
			pass
		self._pool = ThreadedConnectionPool(config.get('database', 'min_database_connections'), config.get('database', 'max_database_connections'), dsn)

	@contextmanager
	def get_cursor(self, commit=True):
		""" Gets a cursor from a connection in the pool """
		connection = self._pool.getconn()
		cursor = connection.cursor(cursor_factory=RigorCursor)
		try:
			yield cursor
		except:
			self.rollback(cursor)
			raise
		else:
			if commit:
				self.commit(cursor)
			else:
				self.rollback(cursor)

	def _close_cursor(self, cursor):
		""" Closes a cursor and releases the connection to the pool """
		cursor.close()
		self._pool.putconn(cursor.connection)

	def commit(self, cursor):
		""" Commits the transaction, then closes the cursor """
		cursor.connection.commit()
		self._close_cursor(cursor)

	def rollback(self, cursor):
		""" Rolls back the transaction, then closes the cursor """
		cursor.connection.rollback()
		self._close_cursor(cursor)

	def __del__(self):
		self._pool.closeall()

class RowMapper(object):
	"""
	Maps a database row (as a dict) to a returned dict, with transformed fields
	as necessary
	"""
	def __init__(self, field_mappings=None, field_transforms=None):
		if field_mappings is None:
			field_mappings = dict()
		if field_transforms is None:
			field_transforms = dict()
		self._field_mappings = field_mappings
		self._field_transforms = field_transforms

	def map_row(self, row):
		"""
		Transforms keys and values as necessary to transform a database row into a
		returned dict
		"""
		new = dict()
		for column_name, value in row.iteritems():
			if self._field_mappings.has_key(column_name):
				key = self._field_mappings[column_name]
			else:
				key = column_name
			if key is None:
				continue
			if self._field_transforms.has_key(key):
				value = self._field_transforms[key](value, column_name, row)
			new[key] = value
		return new

def transactional(function):
	""" Runs a function wrapped in a single transaction """
	def _execute(self, *args, **kwargs):
		real_function_name = "_" + function.__name__
		real_function = getattr(self, real_function_name)
		with self._db.get_cursor() as cursor:
			return real_function(cursor, *args, **kwargs)
	return _execute

def uuid_transform(value, column_name, row):
	""" Returns a UUID object """
	if value is None:
		return None
	return row[column_name].hex

def polygon_transform(value, column_name, row):
	""" Returns a polygon as a list of tuples (points) """
	if value is None:
		return None
	return list(eval(row[column_name]))

def polygon_tuple_adapter(polygon):
	""" Returns a string formatted in a way that PostgreSQL recognizes as a polygon, given a sequence of pairs """
	if polygon is None:
		return None
	return str(tuple([tuple(point) for point in polygon]))
