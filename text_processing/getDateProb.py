#! /usr/bin/env python

"""
generate date frequencies from MapReduce date output
then import generated date freqs into MongoDB as a new field in 'date' collection

To run: python getDateProb.py /path/to/date_aa.txt

Created by Bin Dai & Siyuan Guo, Mar 2014.
"""

from pymongo import MongoClient
from collections import defaultdict
from utils import date2daterange, freq2prob
import sys

def main(filepath):
    # connect to mongoDB and check date collection
    client = MongoClient('localhost', 27017)
    db = client.HTRC
    collections = db.collection_names()
    if "date" not in collections:
        print "Collection 'date' is required. \
        Please run metadata_processing/get_dependent_variable/getDV_HTRC.py first."
        
    # assume that 'date' collection is small enough to put in memory
    with open(filepath) as file:
        old_key = None # tracking doc_id
        tfdict = defaultdict(float) # date term frequency dictionary for each document
        for line in file:
            if line:
                this_key,date,tf = line.split('\t')
                if this_key != old_key and old_key:
                    # to successfully update, use unicode
                    db.date.update({u"_id":unicode(old_key)},{'$set':{"distribution":freq2prob(tfdict)}})
                    tfdict = defaultdict(float)
                old_key = this_key
                # update date tf 
                tfdict[date2daterange(int(date))]+= float(tf)
        # dont forget last doc
        db.date.update({u"_id":unicode(old_key)},{'$set':{"distribution":freq2prob(tfdict)}})

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print "Please provide MapReduce date output file."
	else:
		main(sys.argv[1])
