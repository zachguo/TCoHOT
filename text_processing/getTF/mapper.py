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

import sys,os,re,string

INPUT_FOLDER_NAME = 'HTRCInputFiles' # used for cleaning up working filepath to derive doc_id
SENTENCE_SEPARATORS = r'(?<! [A-Z])\.+\"?\'?(?=\D|$)|\,\"?\'?(?=\D|$)|\!+\"?\'? ?|\?+\"?\'? ?|\;\"?\'? ?|\: ?'

# @params: line<String> is a striped line
def isrubbish(line):
	length, gem = 0.0, 0.0
	for char in line:
		length += 1.0
		if char.isalnum(): gem += 1.0
	if length == 1: return True # too short
	if length<4 and gem<length: return True # short line with rubbish
	if gem/length <= 0.5: return True # more than half of the line is rubbish
	return False

# @params: line<String> is a striped and non-rubbish line; 
#          leftover<String or None> is the leftover of previous line after sentence extraction
def isheaderfooter(line, leftover):
	line = ''.join([c for c in line if c.isalnum()])
	if line.isdigit() and len(line)<4: return True # possibly a page number
	if line.isupper(): 
		if line[0].isdigit() or line[-1].isdigit(): return True # all capitalized with a number at beginning or end
	if leftover and line[0].isupper():
		# leftover is normal content but new line is title-like
		if leftover[-1].islower() and not leftover.split(' ')[-1][0].isupper(): return True
		if leftover.endswith('-'):
			if len(leftover)>1:
				if leftover[-2].isupper(): return False # all capitalized content
				else: return True
	return False

# @params: line<String> is a striped, non-rubbish, and non-header/footer line; 
#          leftover<String> is the leftover of previous line after sentence extraction
def get_sents(line, leftover):
	if leftover:
		if leftover.endswith('-'): leftover = leftover[0:-1]
		elif leftover[-1].islower() and line[0].islower(): leftover += ' '
		elif leftover[-1].isupper() and line[0:5].isupper(): leftover += ' '
		else: leftover += '. '
	sents = re.split(SENTENCE_SEPARATORS, leftover+line) # concate and split
	leftover = sents[-1].strip() # if line ends with a sentence separator, the last element of sents is an empty string
	sents = sents[0:-1]
	return sents, leftover

def print_ngram(sent):
	words = sent.split(' ')
	if words:
		# get doc_id from the filename of currently working file
		if os.environ.has_key('map_input_file'):
			# there's such a key only in hadoop environment or when running runlocal.sh
			doc_id = os.environ['map_input_file'].split(INPUT_FOLDER_NAME+'/')[1].split('.txt')[0]
		else:
			# for local testing
			doc_id = 'fake_doc_id'
		# get ngrams
		for i in range(len(words)):
			for j in range(i+1,min(i+4, len(words))):
				term = ' '.join([w.strip(string.punctuation+' ').lower() for w in words[i:j]])
				# emit key-value pair into stdout, key is a composite key made up of a doc_id, a separator and a term.
				print '{0}[SEP]{1}\t{2}'.format(doc_id,term,1)

def main():
	leftover = ''
	for line in sys.stdin:
		line = line.strip()
		if line:
			if isrubbish(line): continue
			if isheaderfooter(line, leftover): continue
			sents, leftover = get_sents(line, leftover)
			for sent in sents:
				sent = sent.strip(string.punctuation+' ')
				if sent:
					print_ngram(sent)

if __name__ == '__main__':
	main()