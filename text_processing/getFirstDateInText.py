#! /usr/bin/env python

# Created by Siyuan Guo, Mar 2014.

import re,glob,sys
from string import maketrans
from pymongo import MongoClient

digits = re.compile(r'\d')
def hasDigit(word):
	return bool(digits.search(word))

SC4D = re.compile(r'(^|\D+)(\d{4})(\D+|$)') # precompiled pattern for standalone consecutive 4 digits
TYPOTABLE = maketrans('lJQOo','11000')
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

def date2daterange(year):
    if year <= 1839:
        return "pre-1839"
    elif year>=1840 and year <= 1860:
        return "1840-1860"
    elif year>=1861 and year <= 1876:
        return "1861-1876"
    elif year>=1877 and year <= 1887:
        return "1877-1887"
    elif year>=1888 and year <= 1895:
        return "1888-1895"
    elif year>=1896 and year <= 1901:
        return "1896-1901"
    elif year>=1902 and year <= 1906:
        return "1902-1906"
    elif year>=1907 and year <= 1910:
        return "1907-1910"
    elif year>=1911 and year < 1914:
        return "1911-1914"
    elif year>=1915 and year <= 1918:
        return "1915-1918"
    elif year>=1919 and year <= 1922:
        return "1919-1922"
    else:
        return "1923-present"

def main(filepath):
	client = MongoClient('localhost', 27017)
	db = client.HTRC
	collections = db.collection_names()
	if "date" not in collections:
		print "Collection 'date' is required. Please run metadata_processing/get_dependent_variable/getDV_HTRC.py first."

	# scan first date-in-text
	allfilenames = glob.glob(filepath.rstrip('/')+'/*.txt')
	for fn in allfilenames:
		fn_short = fn.split('/')[-1]
		if fn_short.endswith('.txt'):
			doc_id = fn_short.split('.txt')[0]
			seen_date = False
			fin = open(fn)
			line = fin.readline()
			while not seen_date and line:
				words = line.strip().split(' ')
				while words and not seen_date:
					word = words.pop(0)
					date = getDate(word)
					if date:
						seen_date = True
						db.date.update({u"_id":unicode(doc_id)},{'$set':{"firstraw":date, "firstrange":date2daterange(date)}})
				line = fin.readline()
			fin.close()

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print "Please provide the path of text corpora. Yes, that folder cantaining 250k documents."
	else:
		main(sys.argv[1])