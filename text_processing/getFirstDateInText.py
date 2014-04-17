#! /usr/bin/env python

"""
Scan and print first date-in-text.
This script is intended to run locally, to run:
	python getFirstDateInText.py path/to/aa > date1st_aa.txt

Siyuan Guo, Mar 2014.
"""

import re, glob, sys
from string import maketrans

DIGITS = re.compile(r'\d')
def has_digit(word):
	"""Check whether word contains digits"""
	return bool(DIGITS.search(word))

# precompiled pattern for standalone consecutive 4 digits
SC4D = re.compile(r'(^|\D+)(\d{4})(\D+|$)')
TYPOTABLE = maketrans('lJQOo', '11000')
def get_date(word):
	"""Get date from potential date string"""
	if has_digit(word):
		# greedily fix potential OCR typos
		word = word.translate(TYPOTABLE)
		# find standalone consecutive 4 digits, '18888' don't count
		match = SC4D.search(word)
		if match:
			word = int(match.groups()[1])
		# assume all date is later than 1500, to filter noise like address#
		if word > 1400 and word < 2000:
			return word
	return None

def main(filepath):
	"""Scan and print first date-in-text"""
	allfilenames = glob.glob(filepath.rstrip('/')+'/*.txt')
	for fname in allfilenames:
		fname_short = fname.split('/')[-1]
		if fname_short.endswith('.txt'):
			doc_id = fname_short.split('.txt')[0]
			seen_date = False
			fin = open(fname)
			line = fin.readline()
			while not seen_date and line:
				words = line.strip().split(' ')
				while words and not seen_date:
					word = words.pop(0)
					date = get_date(word)
					if date:
						seen_date = True
						print "{0}\t{1}".format(doc_id, date)
				line = fin.readline()
			fin.close()

if __name__ == '__main__':
	if len(sys.argv) != 2:
		raise IOError("Please provide the path of text corpora.")
	else:
		main(sys.argv[1])