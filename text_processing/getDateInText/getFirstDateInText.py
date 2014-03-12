#! /usr/bin/env python

# Created by Siyuan Guo, Mar 2014.

import re,glob,sys

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

def getDateRangeIndex(year):
	year = int(year)
	if year <= 1839:
	    return 0
	elif year>=1840 and year <= 1860:
	    return 1
	elif year>=1861 and year <= 1876:
	    return 2
	elif year>=1877 and year <= 1887:
	    return 3
	elif year>=1888 and year <= 1895:
	    return 4
	elif year>=1896 and year <= 1901:
	    return 5
	elif year>=1902 and year <= 1906:
	    return 6
	elif year>=1907 and year <= 1910:
	    return 7
	elif year>=1911 and year < 1914:
	    return 8
	elif year>=1915 and year <= 1918:
	    return 9
	elif year>=1919 and year <= 1922:
	    return 10
	else:
	    return 11

def main(filepath):
	dr = ["pre-1839_1st","1840-1860_1st","1861-1876_1st","1877-1887_1st","1888-1895_1st","1896-1901_1st","1902-1906_1st","1907-1910_1st","1911-1914_1st","1915-1918_1st","1919-1922_1st","1923-present_1st"]
	dr_num = len(dr)
	allfilenames = glob.glob(filepath.rstrip('/')+'/*.txt')
	with open('DateInText1st_aa.txt','w') as fout:
		fout.write('\t'.join(['doc_id']+dr)+'\n')
		for fn in allfilenames:
			doc_id = re.search(r'[^\/]+(?=\.txt$)',fn).group()
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
						l = ['F']*dr_num
						l[getDateRangeIndex(date)] = 'T'
						fout.write('\t'.join([doc_id]+l)+'\n')
				line = fin.readline()
			if not seen_date:
				fout.write('\t'.join([doc_id]+['F']*dr_num)+'\n')
			fin.close()

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print "Please provide the path of text corpora."
	else:
		main(sys.argv[1])