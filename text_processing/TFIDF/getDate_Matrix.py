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

def freq2prob(tfdict):
    total = sum(tfdict.values())
    return {t:tfdict[t]/total for t in tfdict}

def main(filepath):
    client = MongoClient('localhost', 27017)
    db = client.HTRC
    collections = db.collection_names()
    if "date_tf" in collections:
        print "Collection date_tf already exists in 'HTRC' database."
        return
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
                year = float(date)
                if year <= 1839:
                    tfdict["pre-1839"]+= float(tf)
                elif year>=1840 and year <= 1860:
                    tfdict["1840-1860"]+= float(tf)
                elif year>=1861 and year <= 1876:
                    tfdict["1861-1876"]+= float(tf)
                elif year>=1877 and year <= 1887:
                    tfdict["1877-1887"]+= float(tf)
                elif year>=1888 and year <= 1895:
                    tfdict["1888-1895"]+= float(tf)
                elif year>=1896 and year <= 1901:
                    tfdict["1896-1901"]+= float(tf)
                elif year>=1902 and year <= 1906:
                    tfdict["1902-1906"]+= float(tf)
                elif year>=1907 and year <= 1910:
                    tfdict["1907-1910"]+= float(tf)
                elif year>=1911 and year < 1914:
                    tfdict["1911-1914"]+= float(tf)
                elif year>=1915 and year <= 1918:
                    tfdict["1915-1918"]+= float(tf)
                elif year>=1919 and year <= 1922:
                    tfdict["1919-1922"]+= float(tf)
                else:
                    tfdict["1923-present"]+= float(tf)
                    
        # dont forget last doc
        date_tf.append({"_id":old_key, "dates":freq2prob(tfdict)})

    with open('date_in_text.csv', 'w') as fout:
        for i in db.date_tf.find():
            fout.write(i["_id"])
            dates = i["dates"]
            for date in ["pre-1849","1840-1860","1861-1876","1877-1887","1888-1895","1896-1901","1902-1906","1907-1910","1911-1914","1915-1918","1919-1922","1923-present"]:
                if date in dates:
                    fout.write(',{0}'.format(dates[date]))
                else:
                    fout.write(',0')
            fout.write('\n')
    fout.close()
        

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print "Please provide Date Date-In-Text file."
	else:
		main(sys.argv[1])
