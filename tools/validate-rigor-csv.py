#!/usr/bin/env python

import argparse
import sys

import csvtoolkit as ct
import rigortoolkit as rt

parser = argparse.ArgumentParser(description='Check if CSV file(s) match the standard Rigor CSV format.')
parser.add_argument('paths', nargs='+', type=str, help='Paths to CSV file(s)')
parser.add_argument('-v', '--verbose', action='store_true', help='')
if len(sys.argv) <= 1:
	parser.print_help()
	sys.exit(1)
args = parser.parse_args()

numBadFiles = 0
for path in args.paths:
	try:
		r = ct.getRowStream(path)
		r = rt.validateRows(r)
		ct.runStream(r)
		if args.verbose:
			print('    {}: Valid'.format(path))
	except ct.CSVException, e:
		numBadFiles += 1
		print('    {}'.format(e))
if numBadFiles:
	print('{} files had issues.'.format(numBadFiles))
else:
	print('No problems.')

