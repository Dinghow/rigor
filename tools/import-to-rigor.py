#!/usr/bin/env python

from __future__ import division

import argparse
import random
import sys
import time

import rigor.types
from rigor.config import RigorDefaultConfiguration
from rigor.database import Database
import sqlalchemy.orm.exc

import csvtoolkit as ct
import rigortoolkit as rt
import importer

#================================================================================
# COMMAND LINE

parser = argparse.ArgumentParser(description='Import a Rigor-CSV format file to Rigor.')
parser.add_argument('inFn', type=str, help='Input filename')
parser.add_argument('-c', '--config', type=str, default='bin/.rigor.ini', help='Path to rigor.ini config file.  Defaults to "bin/.rigor.ini".')
parser.add_argument('-v', '--verbose', action='store_true', help='')
parser.add_argument('--overwrite', action='store_true', help='If things already exist in the db with the same unique id ("uid" property), overwrite them instead of skipping the import for those rows.  This is not implemented yet.')  # TODO
parser.add_argument('-n', '--dryrun', action='store_true', help='Don\'t actually commit the changes to the database.')
parser.add_argument('--delay', type=int, default=0, help='Delay the import for up to this many seconds.  Useful when launching many copies of this script in parallel.')
if len(sys.argv) <= 1:
	parser.print_help()
	sys.exit(1)
args = parser.parse_args()

#================================================================================
# TRANSFORM CSV

# TODO: move this to csvtoolkit
def countRows(rowStream, outList):
	if len(outList) == 0:
		outList.append(0)
	for row in rowStream:
		outList[0] += 1
		yield row

try:
	start = time.time()
	if args.dryrun:
		print('DRY RUN')

	if args.delay:
		print('Random delay of up to {} seconds...'.format(args.delay))
		time.sleep(random.random() * args.delay)

	print('Reading from {}'.format(args.inFn))

	# first pass: validate and count rows
	print('First pass: validating Rigor format and counting rows')
	numRows = [0]
	r = ct.getRowStream(args.inFn, encoding='utf8')
	r = countRows(r, numRows)
	r = rt.validateRows(r)
	ct.runStream(r)
	numRows = numRows[0]
	print('{} rows.'.format(numRows))
	print('File has passed Rigor format validation.')

	# second pass: import
	print('Second pass: importing to db')
	r = ct.getRowStream(args.inFn, encoding='utf8')
	# if args.verbose:
	# 	r = ct.showJson(r, divider='----------- row from csv file:')
	r = importer.importToRigor(r, configFn=args.config, dryrun=args.dryrun, verbose=args.verbose, overwrite=args.overwrite, numRows=numRows)
	ct.runStream(r)

	print('{} rows.'.format(numRows))
	seconds = time.time() - start
	print('{:0.1f} minutes ({:0.1f} rows per minute)'.format(seconds/60, numRows / (seconds/60)))
except ct.CSVException, e:
	print(e)
	sys.exit(1)
except KeyboardInterrupt:
	print('Quitting by control-C.  The transaction was not committed unless it says so in the last screenful of output.')
	if args.dryrun:
		print('DRY RUN')
	sys.exit(1)
except Exception, e:
	print('An error occurred.  The transaction was not committed unless it says so in the last screenful of output.')
	if args.dryrun:
		print('DRY RUN')
	raise

if args.dryrun:
	print('DRY RUN')

print('==== {}: DONE ===='.format(args.inFn))

