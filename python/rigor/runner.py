"""
Runs an algorithm across a set of images.  Result is a report containing image
ID, detected model, expected model, and elapsed time
"""
from rigor.config import config
from rigor.dbmapper import DatabaseMapper

import rigor.domain
import rigor.logger
import rigor.database

from multiprocessing.pool import Pool

# TODO: until we make Domains non-objects somehow, allowing a process pool,
# we're better off not using multiprocessing.

class Runner(object):
	""" Class for running algorithms against test images """

	def __init__(self, domain, parameters, limit=None):
		"""
		The domain dictates which algorithm to run (algorithm is fixed per domain),
		and which images to use as sources.  The limit is an optional maximum number
		of images to use as sources
		"""
		if domain not in rigor.domain.kModules:
			raise ValueError("Unknown domain '{0}'".format(domain))
		domain_module = __import__('rigor.domain.{0}'.format(domain), fromlist=['init', 'run'])
		domain_module.init(parameters)
		self._domain = domain
		self._limit = limit
		self._logger = rigor.logger.getLogger('.'.join((__name__, self.__class__.__name__)))
		self._database = rigor.database.Database()
		self._database_mapper = DatabaseMapper(self._database)
		self._domain_module = domain_module
		self._pool = Pool(int(config.get('global', 'max_workers')))

	def set_parameters(self, parameters):
		self._domain_module.set_parameters(parameters)

	def run(self):
		self._logger.debug('Fetching image IDs from database')
		images = self._database_mapper.get_images_for_analysis(self._domain, self._limit)
		self._logger.debug('Processing {0} images'.format(len(images)))
		return self._pool.map(self._domain_module.run, images)
