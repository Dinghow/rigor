""" Configuration """

import os
from ConfigParser import RawConfigParser

kConfigurationPath = os.path.join(os.environ['HOME'], '.vision.ini')

_defaults = dict(
		max_worker_threads = 8,
		copy_local = 'yes',
		database = 'vision',
		ssl = 'yes',
		min_database_connections = 0,
		max_database_connections = 10,
		metadata_file = 'metadata.json',
		timestamp_format = '%Y-%m-%dT%H:%M:%SZ'
	)
config = RawConfigParser(_defaults)
config.read(kConfigurationPath)
