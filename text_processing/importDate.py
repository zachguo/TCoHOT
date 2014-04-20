#! /usr/bin/env python

"""
generate date frequencies from MapReduce date output
then import generated date freqs into MongoDB as a new field in 'date' collection

To run: 
	python importDate.py /path/to/date_aa.txt /path/to/date1st_aa.txt

Created by Bin Dai & Siyuan Guo, Mar 2014.
"""

from pymongo import MongoClient
from collections import defaultdict
from utils import date2daterange, freq2prob
import sys

def main(fp_date, fp_date1st):
	"""Run"""
	# connect to mongoDB and check date collection
	client = MongoClient('localhost', 27017)
	db = client.HTRC
	collections = db.collection_names()
	if "date" not in collections:
		print "Collection 'date' is required. \
		Please run metadata_processing/get_dependent_variable/getDV_HTRC.py first."

	# import date related data into MongoDB
	import_date(fp_date, db)
	import_date1st(fp_date1st, db)

def import_date(fp_date, db):
	"""Import date distribution into MongoDB"""
	# assume that 'date' collection is small enough to put in memory
	with open(fp_date) as fin:
		old_key = None # tracking doc_id
		tfdict = defaultdict(float) # date term frequency dictionary for each document
		for line in fin:
			if line:
				this_key, date, tf = line.strip('\n').split('\t')
				if this_key != old_key and old_key:
					# to successfully update, use unicode
					db.date.update({u"_id":unicode(old_key)}, {'$set':{"distribution":freq2prob(tfdict)}})
					tfdict = defaultdict(float)
				old_key = this_key
				# update date tf 
				tfdict[date2daterange(int(date))] += float(tf)
		# dont forget last doc
		db.date.update({u"_id":unicode(old_key)}, {'$set':{"distribution":freq2prob(tfdict)}})
	print "Finish importing date distributions."

def import_date1st(fp_date1st, db):
	"""Import 1st-date-in-text into MongoDB"""
	with open(fp_date1st) as fin:
		for line in fin:
			if line:
				doc_id, date = line.strip('\n').split('\t')
				db.date.update({u"_id":unicode(doc_id)},{'$set':{"firstraw":date, "firstrange":date2daterange(int(date))}})
	print "Finish importing 1st-date-in-texts."

if __name__ == '__main__':
	if len(sys.argv) != 3:
		raise IOError("Please provide \
                \n1) MapReduce date output file and \
                \n2) output file from getFirstDateInText.py. \
                \n(Two arguments should be given)")
	elif sys.argv[2].find('1st') < 0:
		raise IOError("Second argument must contain '1st'.")
	else:
		main(sys.argv[1], sys.argv[2])
