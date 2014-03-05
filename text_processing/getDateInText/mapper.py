#! /usr/bin/python

# Mapper for Hadoop Streaming
# This mapper reads text files line by line, then emits key-value pairs into stdout
# key-value is separated by tab, and the key is a composite key, an example is as below:
#
# 	loc.ark+=13960=t0bv7jp0r[SEP]1876	1
#
# You can test this script locally by:
#
#   cat test.txt | ./mapper.py | sort | ./reducer.py
#

import sys,os,re

INPUT_FOLDER_NAME = "HTRCInputFiles" # used for cleaning up working filepath to derive doc_id

def hasDigit(word):
	for c in word:
		if c.isdigit():
			return True
	return False

def getDate(word):
	if hasDigit(word):
		# greedily fix potential OCR typos
		for typo,fix in [('l', '1'),('J','1'),('O','0'),('o','0')]:
			word = word.replace(typo, fix)
		# find standalone consecutive 4 digits, '18888' don't count
		match = re.search(r'(^|\D+)(\d{4})(\D+|$)',word)
		if match:
			word = int(match.groups()[1])
		# assume all date is later than 1500, to filter noise like address#
		if word>1400 and word<2000:
			return word
	return None

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
		# get date in text
		for word in words:
			word = getDate(word)
			if word:
				print '{0}[SEP]{1}\t{2}'.format(doc_id,word,1)