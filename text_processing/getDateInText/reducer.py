#! /usr/bin/env python

# Reducer for Hadoop Streaming
# This reducer reads sorted key-value pairs produced by mappers line by line
# (note that the key is a composite key in format "{doc_id}[SEP]{term}" )
# then output into stdout in a format {doc_id}\t{term}\t{frequency}, e.g.
#
# 	loc.ark+=13960=t0bv7jp0r	1876	193
#
# You can test this script locally by:
#
#   cat test.txt | ./mapper.py | sort | ./reducer.py
#

# Created by Siyuan Guo, Mar 2014.

import sys

SEPARATOR = "[SEP]"

old_key = None
count = 0

for line in sys.stdin:
	data_row = line.strip().split('\t')
	if len(data_row) == 2:
		# read composite-key and value
		this_key = data_row[0]
		val = int(data_row[1])
		# aggregate freq count based on key
		if old_key and this_key != old_key:
			# emit {doc_id}\t{term}\t{frequency}
			print '\t'.join(old_key.split(SEPARATOR)+[str(count)])
			count = 0
		old_key = this_key
		count += val
if old_key:
	# don't forget last line
	print '\t'.join(old_key.split(SEPARATOR)+[str(count)])