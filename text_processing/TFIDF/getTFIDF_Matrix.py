#! /usr/bin/env python

# import term frequencies file in MongoDB.
# The collection name of extracted dependent variable is "tf".
# An example folder name is: /path/to/TFfile/
# To run: python importTF.py /path/to/TFfile/

# Created by Bin Dai, Mar 2014.


from pymongo import MongoClient
from collections import defaultdict
import os,re,sys,math

def main(filepath):
    client = MongoClient('localhost', 27017)
    db = client.HTRC
    dfdict = defaultdict(float)
    docNum = db.tf_3.find().count()
    with open(filepath) as fin:
        for line in fin:
            if line:
                term,df = line.split('\t')
                dfdict[term] = float(df)
    termlist = dfdict.keys()
    print len(termlist)
    doc_count = 0 # for printing progress
    with open('tfidf_3.csv', 'w') as fout:
        for doc in db.tf_3.find():
            fout.write(doc["_id"])
            for term in termlist:
                tfidf = float(doc["tfs"].get(term,0))*(math.log10(docNum)-math.log10(dfdict[term]))
                fout.write('\t{0}'.format(tfidf))
            fout.write('\n')
            doc_count += 1
            if doc_count % 2000==0: print doc_count


if __name__ == '__main__':
    if len(sys.argv) != 2:
		print "Please provide DF file."
    else:
        main(sys.argv[1])
