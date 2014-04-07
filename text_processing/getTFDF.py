#! /usr/bin/env python

# This module generate term-frequencies and document-frequencies from MapReduce TF output
# then import generated TF & DF into MongoDB as six collections: 
# 'tf_1', 'tf_2', 'tf_3', 'df_1', 'df_2', 'df_3'

# To run: python getTF.py /path/to/tf_aa.txt

# Created by Bin Dai, Siyuan Guo, Mar 2014.


from pymongo import MongoClient
from collections import defaultdict
import os,re,sys,math

def freq2prob(tfdict):
    # conver raw frequencies to probabilities
    total = sum(tfdict.itervalues())
    return {t:tfdict[t]/total for t in tfdict}

def reshape(dfdict):
    # reshape df dict from {'term':99, ...} into [ { _id:'term', df:99}, ... ]
    return map(lambda (x,y):{'_id':x, 'df':y}, dfdict.items())

def import2mongo(filepath, db):

    collections = db.collection_names()
    for c in ['tf_1','tf_2','tf_3','df_1','df_2','df_3']:
        if c in collections: 
            print "Collection %s already exists in 'HTRC' database. Drop it." % c
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
                    tf_uni.append({"_id":old_key, "freq":tfdict_uni, "prob":freq2prob(tfdict_uni)})
                    tf_bi.append({"_id":old_key, "freq":tfdict_bi, "prob":freq2prob(tfdict_bi)})
                    tf_tri.append({"_id":old_key, "freq":tfdict_tri, "prob":freq2prob(tfdict_tri)})
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
        tf_uni.append({"_id":old_key, "freq":tfdict_uni, "prob":freq2prob(tfdict_uni)})
        tf_bi.append({"_id":old_key, "freq":tfdict_bi, "prob":freq2prob(tfdict_bi)})
        tf_tri.append({"_id":old_key, "freq":tfdict_tri, "prob":freq2prob(tfdict_tri)})
        # insert regardless of count
        db.tf_1.insert(tf_uni)
        db.tf_2.insert(tf_bi)
        db.tf_3.insert(tf_tri)

    # save df (document frequencies) to collections ('df_1','df_2','df_3')
    db.df_1.insert(reshape(dfdict_uni))
    db.df_2.insert(reshape(dfdict_bi))
    db.df_3.insert(reshape(dfdict_tri))

def main(filepath):
    client = MongoClient('localhost', 27017)
    db = client.HTRC
    import2mongo(filepath, db)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Please provide MapReduce TF output file."
    else:
        main(sys.argv[1])