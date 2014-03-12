#! /usr/bin/env python

# This module compute TF and DF based on mapreduce tf output, then 
# import computed term frequencies file into MongoDB, 
# three collections 'tf_1', 'tf_2' and 'tf_3' will be created in MongoDB, 
# representing unigram, bigram, and trigram respectively.
# Also, three text files will be created to store document frequencies.

# To run: python importTF.py /path/to/tf_aa.txt

# Created by Bin Dai, Siyuan Guo, Mar 2014.

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
    for c in ['tf_1','tf_2','tf_3']:
        if c in collections: 
            print "Collection {0} already exists in 'HTRC' database. Drop it.".format(c)
            db.drop_collection(c)

    count = 0 # use for bulk insert without using up memory
    dfdict_uni,dfdict_bi,dfdict_tri = defaultdict(float),defaultdict(float),defaultdict(float) # document frequencies of terms
    tf_uni,tf_bi,tf_tri = [],[],[] # list of documents
    with open(filepath) as file:
        old_key = None # tracking doc_id
        tfdict_uni,tfdict_bi,tfdict_tri = {},{},{} # term frequency dictionary for each document
        for line in file:
            if line:
                this_key,term,tf = line.split('\t')
                term = term.replace('.','-') # BSON doesn't allow '.' and '$'
                term = term.replace('$','-')
                if this_key != old_key and old_key:
                    tf_uni.append({"_id":old_key, "tfs":freq2prob(tfdict_uni)})
                    tf_bi.append({"_id":old_key, "tfs":freq2prob(tfdict_bi)})
                    tf_tri.append({"_id":old_key, "tfs":freq2prob(tfdict_tri)})
                    tfdict_uni,tfdict_bi,tfdict_tri = {},{},{}
                    count += 1
                    if count>2000: # 2000 docs as a batch
                        db.tf_1.insert(tf_uni)
                        db.tf_2.insert(tf_bi)
                        db.tf_3.insert(tf_tri)
                        # clear memory & count
                        tf_uni,tf_bi,tf_tri = [],[],[]
                        count = 0

                old_key = this_key
                # update tf & df
                if term.count(' ')==0:
                    tfdict_uni[term] = float(tf)
                    dfdict_uni[term] += 1
                elif term.count(' ')==1:
                    tfdict_bi[term] = float(tf)
                    dfdict_bi[term] += 1
                elif term.count(' ')==2:
                    tfdict_tri[term] = float(tf)
                    dfdict_tri[term] += 1

        # dont forget last doc
        tf_uni.append({"_id":old_key, "tfs":freq2prob(tfdict_uni)})
        tf_bi.append({"_id":old_key, "tfs":freq2prob(tfdict_bi)})
        tf_tri.append({"_id":old_key, "tfs":freq2prob(tfdict_tri)})
        # insert regardless of count
        db.tf_1.insert(tf_uni)
        db.tf_2.insert(tf_bi)
        db.tf_3.insert(tf_tri)

    for i in range(3):
        with open('df_'+str(i+1), 'w') as fout:
            dfs = [dfdict_uni,dfdict_bi,dfdict_tri]
            for k in dfs[i]:
                fout.write('{0}\t{1}\n'.format(k,dfs[i][k]))


if __name__ == '__main__':
	if len(sys.argv) != 2:
		print "Please provide TF file."
	else:
		main(sys.argv[1])
