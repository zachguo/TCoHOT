#! /usr/bin/env python

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
# You can also generate production results locally by modify&run runlocal.sh

# Created by Siyuan Guo, Mar 2014.

import sys,re
from os import environ
from string import maketrans

digits = re.compile(r'\d')
def hasDigit(word):
	return bool(digits.search(word))

SC4D = re.compile(r'(^|\D+)(\d{4})(\D+|$)') # precompiled pattern for standalone consecutive 4 digits
TYPOTABLE = maketrans('lJOo','1100')
def getDate(word):
	if hasDigit(word):
		# greedily fix potential OCR typos
		word = word.translate(TYPOTABLE)
		# find standalone consecutive 4 digits, '18888' don't count
		match = SC4D.search(word)
		if match:
			word = int(match.groups()[1])
		# assume all date is later than 1500, to filter noise like address#
		if word>1400 and word<2000:
			return word
	return None

for line in sys.stdin:
	words = line.strip().split(' ')
	if words:
		if environ.has_key('map_input_file'):
			# there's such a key only in hadoop environment or by runlocal.sh
			fn = environ['map_input_file'].rpartition('/')[-1]
		else:
			# for local testing
			fn = 'fake_doc_id.txt'
		# get doc_id from the filename of currently working file, non-txt file will return empty string
		doc_id = fn.rpartition('.txt')[0]
		if doc_id:
			# get date in text
			for word in words:
				word = getDate(word)
				if word:
					print '%s[SEP]%s\t%s' % (doc_id,word,1)