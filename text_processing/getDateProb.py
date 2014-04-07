#! /usr/bin/env python

# generate date frequencies from MapReduce date output
# then import generated date freqs into MongoDB as a new field in 'date' collection

# To run: python getDateProb.py /path/to/date_aa.txt

# Created by Bin Dai & Siyuan Guo, Mar 2014.

from pymongo import MongoClient
from collections import defaultdict
import sys

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

def freq2prob(tfdict):
    total = sum(tfdict.values())
    return {t:tfdict[t]/total for t in tfdict}

def main(filepath):
    client = MongoClient('localhost', 27017)
    db = client.HTRC
    collections = db.collection_names()
    if "date" not in collections:
        print "Collection 'date' is required. Please run metadata_processing/get_dependent_variable/getDV_HTRC.py first."
        
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
