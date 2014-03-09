#! /usr/bin/env python

# import term frequencies file in MongoDB.
# The collection name of extracted dependent variable is "tf".
# An example folder name is: /path/to/TFfile/
# To run: python importTF.py /path/to/TFfile/

# Created by Bin Dai, Mar 2014.

from pymongo import MongoClient
import os,re,sys

def main(filepath):
    client = MongoClient('localhost', 27017)
    db = client.HTRC
    collections = db.collection_names()
    if "tf_1" in collections:
        print "Collection 'tf_1' already exists in 'HTRC' database."
    elif "tf_2" in collections:
        print "Collection 'tf_2' already exists in 'HTRC' database."
    elif "tf_3" in collections:
        print "Collection 'tf_3' already exists in 'HTRC' database."
    else:
        file = open(filepath)
        try:
            while 1:
                lines = file.readlines(100000)
                if not lines:
                    break
                for line in lines:
                    arr = line.split('\t')
                    doc = {"_doc_id":arr[0], "term": arr[1], "TF":int(arr[2])}
                    if(arr[1].count(' ')==0):
                        db.tf_1.insert(doc)
                    elif(arr[1].count(' ')==1):
                        db.tf_2.insert(doc)
                    else:
                        db.tf_3.insert(doc)
        finally:
            file.close()

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print "Please provide TF output file path."
	else:
		main(sys.argv[1])
