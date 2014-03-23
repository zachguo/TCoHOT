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
# You can also generate production results locally by modify&run runlocal.sh

# Created by Siyuan Guo, Mar 2014.

import re
from sys import stdin
from os import environ

SENTENCE_SEPARATORS = re.compile(r'(?<! [A-Z])\.+\"?\'?(?=\D|$)|\,\"?\'?(?=\D|$)|\!+\"?\'? ?|\?+\"?\'? ?|\;\"?\'? ?|\: ?') # precompiled pattern for separating sentences
NONALNUM = ''.join(c for c in map(chr, range(256)) if not c.isalnum()) # all non-alphanumerics

# @params: line<String> is a striped line
def isrubbish(line):
	length = len(line)
	gem = len(line.translate(None, NONALNUM))
	if length <= 1: return True # too short
	elif length<4 and gem<length: return True # short line with rubbish
	elif float(gem)/length <= 0.5: return True # more than half of the line is rubbish
	return False

# @params: line<String> is a striped and non-rubbish line; 
#          leftover<String or None> is the leftover of previous line after sentence extraction
def isheaderfooter(line, leftover):
	line = line.translate(None, NONALNUM)
	if line.isdigit() and len(line)<4: return True # possibly a page number
	if line.isupper(): 
		if line[0].isdigit() or line[-1].isdigit(): return True # all capitalized with a number at beginning or end
	if leftover and line[0].isupper():
		# leftover is normal content but new line is title-like
		if leftover[-1].islower() and not leftover.rpartition(' ')[-1][0].isupper(): return True
		if leftover.endswith('-') and len(leftover)>1:
			if not leftover[-2].isupper(): return True # not all capitalized text content
	return False

# @params: line<String> is a striped, non-rubbish, and non-header/footer line; 
#          leftover<String> is the leftover of previous line after sentence extraction
def get_sents(line, leftover):
	if leftover:
		if leftover.endswith('-'): line = ''.join([leftover[:-1],line])
		elif leftover[-1].islower() and line[0].islower(): line = ' '.join([leftover,line])
		elif leftover[-1].isupper() and line[:5].isupper(): line = ' '.join([leftover,line])
		else: line = '. '.join([leftover,line])
	sents = SENTENCE_SEPARATORS.split(line) # split concatenated new text into sentences
	leftover = sents[-1] # if line ends with a sentence separator, the last element of sents is an empty string
	sents = sents[:-1]
	return sents, leftover

def print_ngram(sent):
	words = [w.strip(NONALNUM+' ') for w in sent.lower().split(' ')]
	numw = len(words)
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
			# get ngrams
			for i in xrange(numw):
				for j in xrange(i+1, min(i+4, numw)):
					term = ' '.join(words[i:j])
					# emit key-value pair into stdout, key is a composite key made up of a doc_id, a separator and a term.
					print '%s[SEP]%s\t%s' % (doc_id,term,1)

def main():
	leftover = ''
	for line in stdin:
		line = line.strip()
		if line:
			if isrubbish(line): continue
			if isheaderfooter(line, leftover): continue
			sents, leftover = get_sents(line, leftover)
			for sent in sents:
				sent = sent.strip(NONALNUM+' ')
				if sent:
					print_ngram(sent)

if __name__ == '__main__':
	main()
	# import cProfile
	# cProfile.run('main()')