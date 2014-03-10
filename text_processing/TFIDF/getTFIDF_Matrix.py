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
    termList = []
    docNum = db.tf_1.find().count()
    with open(filepath) as file:
        for line in file:
            if line:
                term, df =  line.split('\t')
                dfdict[term] = float(df)
                termList.append(term)
    file.close()
    with open('tfidf_1.csv', 'w') as fout:
        for i in db.tf_1.find():
            fout.write(i["_id"])
            tfs = i["tfs"]
            for term in termList:
                if term in tfs:
                    tfidf = float(tfs[term])*(math.log10(docNum)-math.log10(dfdict[term]))
                    fout.write(',{0}'.format(tfidf))
                else:
                    fout.write(',0')
            fout.write('\n')
    fout.close()                


if __name__ == '__main__':
    if len(sys.argv) != 2:
		print "Please provide DF file."
    else:
        main(sys.argv[1])
