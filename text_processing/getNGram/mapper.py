#! /usr/bin/python

# Mapper for Hadoop Streaming
# This mapper reads text files line by line, then emits key-value pairs into stdout
# key-value is separated by tab, and the key is a composite key, an example is as below:
#
# 	loc.ark+=13960=t0bv7jp0r[SEP]we	1
#

import sys,os

SEPARATOR = "[SEP]" # used for composite key {doc_id}[SEP]{term}, e.g. "loc.ark+=13960=t0bv7jp0r[SEP]we"
INPUT_FOLDER_NAME = "HTRCInputFiles" # used for cleaning up working filepath to derive doc_id

for line in sys.stdin:
	words = line.strip().split(' ')
	if words:
		# get doc_id from the filename of currently working file
		doc_id = os.environ['map_input_file'].split(INPUT_FOLDER_NAME+'/')[1].split('.txt')[0]
		# get ngrams
		# This is a temporary and simplistic model to test basic functionalities. No tokenization or sentence detection etc.
		for i in range(len(words)):
			for j in range(i+1,min(i+3, len(words))):
				term = ' '.join(words[i:j]).lower()
				# emit key-value pair into stdout, key is a composite key made up of a doc_id, a separator and a term.
				print doc_id+SEPARATOR+term, '\t', 1