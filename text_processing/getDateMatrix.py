#! /usr/bin/env python

# This module combine date frequency by date range, then 
# import date frequencies file into MongoDB, 
# collections 'date_tf' will be created in MongoDB, 
# Also, a matrix(doc, date range) files will be created to store date frequencies.

# To run: python getDate_Matrix.py /path/to/date_aa.txt

# Created by Bin Dai, Mar 2014.

from pymongo import MongoClient
from collections import defaultdict
import os,re,sys

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
    if "date_tf" in collections:
        print "Collection date_tf already exists in 'HTRC' database. Drop it."
        db.drop_collection("date_tf")
        
    count = 0 # use for bulk insert without using up memory
    date_tf = [] # list of documents
    with open(filepath) as file:
        old_key = None # tracking doc_id
        tfdict = defaultdict(float) # date term frequency dictionary for each document
        for line in file:
            if line:
                this_key,date,tf = line.split('\t')
                if this_key != old_key and old_key:
                    date_tf.append({"_id":old_key, "dates":freq2prob(tfdict)})
                    tfdict = defaultdict(float)
                    count += 1
                    if count>2000: # 2000 docs as a batch
                        db.date_tf.insert(date_tf)
                        # clear memory & count
                        date_tf = []
                        count = 0

                old_key = this_key
                # update date tf 
                tfdict[date2daterange(int(date))]+= float(tf)

        # dont forget last doc
        date_tf.append({"_id":old_key, "dates":freq2prob(tfdict)})
        db.date_tf.insert(date_tf)

    dr = ["pre-1839","1840-1860","1861-1876","1877-1887","1888-1895","1896-1901","1902-1906","1907-1910","1911-1914","1915-1918","1919-1922","1923-present"]
    with open('date_in_text.csv', 'w') as fout:
        fout.write('\t'.join(['doc_id']+dr)+'\n')
        for i in db.date_tf.find():
            fout.write(i["_id"])
            dates = i["dates"]
            for date in dr:
                if date in dates:
                    fout.write('\t{0}'.format(dates[date]))
                else:
                    fout.write('\t0')
            fout.write('\n')
        

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print "Please provide Date Date-In-Text file."
	else:
		main(sys.argv[1])
