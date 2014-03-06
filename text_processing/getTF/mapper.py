#! /usr/bin/env python

# Mapper for Hadoop Streaming
# This mapper reads text files line by line, then emits key-value pairs into stdout
# key-value is separated by tab, and the key is a composite key, an example is as below:
#
# 	loc.ark+=13960=t0bv7jp0r[SEP]we	1
#
# You can test this script locally by:
#
#   cat test.txt | ./mapper.py | sort | ./reducer.py
#

# Created by Siyuan Guo, Mar 2014.

import sys,os

INPUT_FOLDER_NAME = "HTRCInputFiles" # used for cleaning up working filepath to derive doc_id

for line in sys.stdin:
	words = line.strip().split(' ')
	if words:
		if os.environ.has_key('map_input_file'):
			# there's such a key only in hadoop environment
			# get doc_id from the filename of currently working file
			doc_id = os.environ['map_input_file'].split(INPUT_FOLDER_NAME+'/')[1].split('.txt')[0]
		else:
			# for local testing
			doc_id = 'fake_doc_id'
		# get ngrams
		# This is a temporary and simplistic model to test basic functionalities. No tokenization or sentence detection etc.
		for i in range(len(words)):
			for j in range(i+1,min(i+4, len(words))):
				term = ' '.join(words[i:j]).lower()
				# emit key-value pair into stdout, key is a composite key made up of a doc_id, a separator and a term.
				print '{0}[SEP]{1}\t{2}'.format(doc_id,term,1)