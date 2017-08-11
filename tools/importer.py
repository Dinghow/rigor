
from __future__ import division

import datetime
import json
import pprint
import time

import rigor.types
from rigor.config import RigorDefaultConfiguration
from rigor.database import Database
import sqlalchemy.orm.exc

def importToRigor(rowStream, configFn, dryrun=False, verbose=False, overwrite=False, numRows=None):
	# set up database
	kConfig = rigor.config.RigorDefaultConfiguration(configFn)
	kDbName = 'YOUR_DB_NAME_HERE'
	db = Database(kDbName, kConfig)

	print('Connecting to database...')
	with db.get_session(commit=not dryrun) as session:
		print('...connected')
		print('Importing rows...')

		lastHash = None
		lastLocator = None
		lastPerceptId = None
		start = time.time()
		for ii, row in enumerate(rowStream):

			# cast everything to a string, in case we're getting live Python objects
			# instead of reading from a file
			# TODO: move this to csvtoolkit
			for k, v in row.data.items():
				if v is None: v = '' # CSV module saves None as ''
				row.data[k] = str(v)

			if row.data['_kind'] == 'percept':
				#---------------------------------------- PERCEPT

				# check for existing percept with same 'hash'
				hash = row.data['p:hash']
				if hash:
					sqlExistingPercept = session.query(rigor.types.Percept).filter(rigor.types.Percept.hash == hash).first()
					if sqlExistingPercept:
						if overwrite:
							print('TODO: overwrite')
							raise NotImplementedError
						else:
							# skip this row
							#print('WARNING: {} line {}: the percept hash "{}" is already in the Rigor database.  Skipping this percept.'.format(row.filename, row.lineNumber, hash))
							continue

				# build percept
				byte_count = row.data['p:byte_count']
				if byte_count == '': byte_count = 0

				percept = rigor.types.Percept()
				session.add(percept)
				percept.byte_count = byte_count
				percept.credentials = row.data['p:credentials']
				percept.device_id = row.data['p:device_id']
				percept.format = row.data['p:format']
				percept.hash = row.data['p:hash']
				percept.locator = row.data['p:locator']
				percept.stamp = datetime.datetime.strptime(row.data['p:stamp'], "%Y-%m-%dT%H:%M:%SZ" )
				percept.x_size = row.data['p:x_size'] or None
				percept.y_size = row.data['p:y_size'] or None
				# sample_count, sample_rate, sensors, collections
				# tags, properties

				# tags
				tags = row.data['p:tags']
				if tags:
					tags = tags.split(',')
					for tag in tags:
						percept.tags.append(rigor.types.PerceptTag(tag))

				# properties
				for fieldName, value in row.data.items():
					if not fieldName.startswith('p:property:'):
						continue
					propertyName = fieldName.replace('p:property:', '')
					if propertyName == '' or value == '':
						continue
					percept.properties[propertyName] = rigor.types.PerceptProperty(name=propertyName, value=value)
				if verbose:
					print('percept ready for the database:')
					print(pprint.pformat(percept.serialize()))

				#---------------------------------------- END PERCEPT
			elif row.data['_kind'] == 'annotation':
				#---------------------------------------- ANNOTATION

				# check for existing annotation with same 'uid' property
				uid = row.data['a:property:uid']
				if uid:
					sqlExistingAnnotation = session.query(rigor.types.AnnotationProperty).filter(rigor.types.AnnotationProperty.name == 'uid', rigor.types.AnnotationProperty.value == uid).first()
					if sqlExistingAnnotation:
						if overwrite:
							print('TODO: overwrite')
							raise NotImplementedError
						else:
							# skip this row
							# print('WARNING: {} line {}: the annotation uid "{}" is already in the Rigor database.  Skipping this annotation.'.format(row.filename, row.lineNumber, uid))
							continue

				# find the percept id to attach this annotation to
				perceptId = None
				hash = row.data['a:percept_hash']
				locator = row.data['a:percept_locator']
				if bool(hash) == bool(locator):
					raise Exception('{}: line {}: annotation must have either a:percept_hash or a:percept_locator, but not both or neither.'.format(row.filename, row.lineNumber))
				if hash:
					if lastHash == hash:
						perceptId = lastPerceptId
					else:
						try:
							sqlPercept = session.query(rigor.types.Percept).filter(rigor.types.Percept.hash == hash).one()
							perceptId = sqlPercept.id
							lastHash = hash
							lastPerceptId = perceptId
							lastLocator = None
						except sqlalchemy.orm.exc.NoResultFound:
							raise Exception('ERROR: {} line {}: percept hash from CSV not found in Rigor db: {}'.format(row.filename, row.lineNumber, hash))
				elif locator:
					print('TODO: look up percept id from a:percept_locator')
					raise NotImplementedError

				# build annotation
				annotation = rigor.types.Annotation()
				session.add(annotation)
				if row.data['a:boundary'] != '':
					boundary = json.loads(row.data['a:boundary'])
					if boundary:
						annotation.boundary = boundary
				if row.data['a:confidence'] != '':
					annotation.confidence = int(row.data['a:confidence'])
				annotation.domain = row.data['a:domain']
				annotation.percept_id = perceptId
				annotation.model = row.data['a:model']

				# parse iso6801 formatted timestamp in UTC ('Z' suffix)
				annotation.stamp = datetime.datetime.strptime(row.data['a:stamp'], "%Y-%m-%dT%H:%M:%SZ" )

				# tags
				tags = row.data['a:tags']
				if tags:
					tags = tags.split(',')
					for tag in tags:
						annotation.tags.append(rigor.types.AnnotationTag(tag))

				# properties
				for fieldName, value in row.data.items():
					if not fieldName.startswith('a:property:'):
						continue
					propertyName = fieldName.replace('a:property:', '')
					if propertyName == '' or value == '':
						continue
					annotation.properties[propertyName] = rigor.types.AnnotationProperty(name=propertyName, value=value)
				if verbose:
					print('annotation ready for the database:')
					print(pprint.pformat(annotation.serialize()))

				#---------------------------------------- ANNOTATION
			else: # _kind is neither annotation nor percept
				raise Exception('{}: line {}: Unknown value in "_kind" column: "{}".  Should be "percept" or "annotation".'.format(row.filename, row.lineNumber, row.data['_kind']))

			# flush the session occasionally
			# this prevents a very large sqlalchemy session from building up locally, filling up memory
			if (ii+1) % 10 == 0:
				if numRows:
					seconds = time.time() - start
					secondsPerRow = seconds/(ii+1)
					remainingRows = numRows - (ii+1)
					secondsLeft = remainingRows * secondsPerRow
					rowsPerMinute = 60 / max(secondsPerRow, 0.001)
					print('{}: {} of {} complete.  {} minutes left.  {:0.1f} rows per minute.'.format(row.filename, ii+1, numRows, int(secondsLeft/60), rowsPerMinute))
				else:
					print('{} complete'.format(ii+1))
				session.flush()
				if not dryrun:
					session.commit()

			yield row

		# done consuming rows from the stream.
		# finished adding all the rows.  now flush the session.
		if dryrun:
			print('Rolling back (DRY RUN)...')
		else:
			print('Flushing one more time, then committing...')
			session.flush()


	# with statement is over
	if dryrun:
		print('Rolled back transaction.')
	else:
		print('Transaction was committed.')
