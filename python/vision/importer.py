"""
Imports images into the database, populating it with metadata where
appropriate.  One-shot; an Importer should be created for each directory to be
imported.
"""

import vision.logger
import vision.database
import vision.constants
import vision.hash
import vision.exceptions

from vision.model.image import Image
from vision.model.annotation import Annotation
from vision.objectmapper import ObjectMapper
from datetime import datetime
from psycopg2 import IntegrityError

import pyexiv2
import Image as PILImage

import os
import uuid
import json
import shutil
import errno

class Importer(object):
	""" Class containing methods for importing images into the Vision framework """

	extensions = ('jpg', 'png')
	""" List of file extensions to scan """

	def __init__(self, directory, database, host, username=None, password=None, move=False):
		self._directory = directory
		self._logger = vision.logger.getLogger('.'.join((__name__, self.__class__.__name__)))
		self._move = move
		self._metadata = dict()
		self._database = vision.database.Database(database, host, username, password)
		self._object_mapper = ObjectMapper(self._database)

	def run(self):
		""" Imports all images from the directory, returning the number processed """
		self._read_global_metadata()
		for entry in os.listdir(self._directory):
			absfile = os.path.abspath(os.path.join(self._directory, entry))
			if not os.path.isfile(absfile):
				# Not a file
				continue
			(basename, sep, extension) = entry.rpartition(os.extsep)
			if not sep:
				# No separating dot
				self._logger.warn("Could not find separator for {0}".format(entry))
				continue
			if extension.lower() not in self.extensions:
				# Extension not in known list
				continue
			# Looks like we have an image file
			self._import_image(absfile, basename)

	def _import_image(self, path, basename):
		""" Reads the metadata for an invididual image and returns an object ready to insert """
		image = vision.model.image.Image()
		image.locator = uuid.uuid4().hex
		image.hash = vision.hash.hash(path)

		data = PILImage.open(path)
		image.resolution = data.size
		image.format = data.format.lower()
		image.depth = Image.modes[data.mode]

		metadata = self._metadata.copy()
		metadata.update(self._read_local_metadata(basename))
		if 'timestamp' in metadata:
			image.stamp = datetime.strptime(metadata['timestamp'], vision.constants.kTimestampFormat)
		else:
			image.stamp = datetime.utcfromtimestamp(os.path.getmtime(path))

		if 'sensor' in metadata:
			image.sensor = metadata['sensor']
		else:
			exif = pyexiv2.ImageMetadata(path)
			exif.read()
			if 'Exif.Image.Make' in exif and 'Exif.Image.Model' in exif:
				image.sensor = ' '.join((exif['Exif.Image.Make'].value, exif['Exif.Image.Model'].value))

		for key in ('location', 'source', 'tags'):
			if key in metadata:
				setattr(image, key, metadata[key])

		annotations = list()
		if 'annotations' in metadata:
			for annotation in metadata['annotations']:
				a = Annotation()
				for key in ('boundary', 'domain', 'rank', 'model', 'value'):
					if key in annotation:
						setattr(a, key, annotation[key])
				if 'timestamp' in annotation:
					a.stamp = datetime.strptime(annotation['timestamp'], vision.constants.kTimestampFormat)
				else:
					a.stamp = image.stamp
				annotations.append(a)
		image.annotations = annotations

		destination = os.path.join(vision.constants.kImageDirectory, image.locator[0:2], image.locator[2:4], os.extsep.join((image.locator, image.format)))
		# We take control of the transaction here so we can fail if copying/moving the file fails
		cursor = self._object_mapper._db.get_cursor()
		try:
			self._object_mapper._create_image(cursor, image)
			# Create destination directory, if it doesn't already exist.  try/except
			# structure avoids race condition
			try:
				os.makedirs(os.path.dirname(destination))
			except OSError as err:
				if err.errno == errno.EEXIST:
					pass
				else:
					raise
			if self._move:
				shutil.move(path, destination)
			else:
				shutil.copy2(path, destination)
			self._object_mapper._db.commit(cursor)
			self._logger.info("Imported image {0}".format(image.locator))
			return image
		except IntegrityError as e:
			self._object_mapper._db.rollback(cursor)
			self._logger.warn("The image at '{0}' already exists in the database".format(path))
		except:
			self._object_mapper._db.rollback(cursor)
			raise

	def _read_local_metadata(self, basename):
		""" Reads the metadata file for the image and sets defaults """
		metadata_file = "{0}.json".format(basename)
		if not os.path.isfile(metadata_file):
			return dict()
		return json.load(open(metadata_file, 'r'))

	def _read_global_metadata(self):
		""" Reads the metadata file for the image directory and sets defaults """
		metadata_file = os.path.join(self._directory, vision.constants.kMetadataFile)
		if not os.path.isfile(metadata_file):
			return
		self._metadata = json.load(open(metadata_file, 'r'))

