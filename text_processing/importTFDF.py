#! /usr/bin/env python

"""
This module generate term-frequencies, document-frequencies and 
character-frequencies from MapReduce TF output, then import generated 
TF, DF & CF into MongoDB as seven collections: 
    'tf_1', 'tf_2', 'tf_3', 'df_1', 'df_2', 'df_3', 'cf'

To run: 
    python importTFDF.py /path/to/tf_aa.txt

Created by Bin Dai, Siyuan Guo, Mar 2014.
"""


from pymongo import MongoClient
from collections import defaultdict
from utils import freq2prob
import sys, codecs


def reshape(dfdict):
    """reshape from {'term':99, ...} into [ { _id:'term', df:99}, ... ]"""
    return [{'_id':x, 'df':y} for x, y in dfdict.items()]


def import2mongo(filepath):

    client = MongoClient('localhost', 27017)
    db = client.HTRC
    collections = db.collection_names()
    for c in ['tf_1', 'tf_2', 'tf_3', 'df_1', 'df_2', 'df_3', 'cf']:
        if c in collections: 
            print "Collection %s already exists in 'HTRC' database. Drop it." % c
            db.drop_collection(c)

    count = 0 # use for bulk insert without using up memory
    dfdict_uni, dfdict_bi, dfdict_tri = defaultdict(float), defaultdict(float), defaultdict(float) # document frequencies of terms
    tf_uni, tf_bi, tf_tri = [], [], [] # list of documents
    chardoclist = []
    with codecs.open(filepath, encoding='utf8') as fin:
        old_key = None # tracking doc_id
        tfdict_uni, tfdict_bi, tfdict_tri = {}, {}, {} # term frequency dictionary for each document
        chardict = defaultdict(float)
        for line in fin:
            if line:
                this_key, term, tf = line.split('\t')
                term = term.replace('.', '-').replace('$', '-') # BSON doesn't allow '.' and '$'
                if this_key != old_key and old_key:
                    tf_uni.append({"_id":old_key, "freq":tfdict_uni, "prob":freq2prob(tfdict_uni)})
                    tf_bi.append({"_id":old_key, "freq":tfdict_bi, "prob":freq2prob(tfdict_bi)})
                    tf_tri.append({"_id":old_key, "freq":tfdict_tri, "prob":freq2prob(tfdict_tri)})
                    chardoclist.append({"_id":old_key, "freq":chardict, "prob":freq2prob(chardict)})
                    tfdict_uni, tfdict_bi, tfdict_tri = {}, {}, {}
                    chardict = defaultdict(float)
                    count += 1
                    if count > 2000: # 2000 docs as a batch
                        db.tf_1.insert(tf_uni)
                        db.tf_2.insert(tf_bi)
                        db.tf_3.insert(tf_tri)
                        db.cf.insert(chardoclist)
                        # clear memory & count
                        tf_uni, tf_bi, tf_tri = [], [], []
                        chardoclist = []
                        count = 0

                old_key = this_key
                # update tf & df
                if term.count(' ') == 0:
                    tfdict_uni[term] = float(tf)
                    dfdict_uni[term] += 1
                    for char in term:
                        chardict[char] += float(tf)
                elif term.count(' ') == 1:
                    tfdict_bi[term] = float(tf)
                    dfdict_bi[term] += 1
                elif term.count(' ') == 2:
                    tfdict_tri[term] = float(tf)
                    dfdict_tri[term] += 1

        # dont forget last doc
        tf_uni.append({"_id":old_key, "freq":tfdict_uni, "prob":freq2prob(tfdict_uni)})
        tf_bi.append({"_id":old_key, "freq":tfdict_bi, "prob":freq2prob(tfdict_bi)})
        tf_tri.append({"_id":old_key, "freq":tfdict_tri, "prob":freq2prob(tfdict_tri)})
        chardoclist.append({"_id":old_key, "freq":chardict, "prob":freq2prob(chardict)})
        # insert regardless of count
        db.tf_1.insert(tf_uni)
        db.tf_2.insert(tf_bi)
        db.tf_3.insert(tf_tri)
        db.cf.insert(chardoclist)

    # save df (document frequencies) to collections ('df_1','df_2','df_3')
    db.df_1.insert(reshape(dfdict_uni))
    db.df_2.insert(reshape(dfdict_bi))
    db.df_3.insert(reshape(dfdict_tri))


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Please provide MapReduce TF output file."
    else:
        import2mongo(sys.argv[1])
        