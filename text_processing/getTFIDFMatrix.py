#! /usr/bin/env python

# This module compute TFIDF given MapReduce TF output

# Term frequencies are first computed and then imported into MongoDB, as three collections: 
# 'tf_1', 'tf_2' and 'tf_3', representing unigram, bigram, and trigram respectively.
# Also, document frequencies are computed along the way and saved into three text files.
# Finally, compute TFIDF and output computed matrix to a tsv file

# To run: python getTFIDFMatrix.py /path/to/tf_aa.txt

# Created by Bin Dai, Siyuan Guo, Mar 2014.


from pymongo import MongoClient
from collections import defaultdict
import os,re,sys,math

def freq2prob(tfdict):
    total = sum(tfdict.itervalues())
    return {t:tfdict[t]/total for t in tfdict}

def importTF(filepath, db):

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

    # save df (document frequencies) to files ('df_1','df_2','df_3')
    for i in range(3):
        with open('df_'+str(i+1), 'w') as fout:
            dfs = [dfdict_uni,dfdict_bi,dfdict_tri]
            for k in dfs[i]:
                fout.write('{0}\t{1}\n'.format(k,dfs[i][k]))

def computeTFIDF(db, dfpaths):
    dfdict = defaultdict(float)
    tables = [db.tf_1, db.tf_2, db.tf_3]
    for i in range(len(tables)):
        alldocs = tables[i].find()
        docNum = alldocs.count()
        with open(dfpaths[i]) as fin:
            for line in fin:
                if line:
                    term,df = line.split('\t')
                    dfdict[term] = float(df)
        termlist = dfdict.keys()
        print 'Number of {0}gram: {1}'.format(i+1,len(termlist))
        doc_count = 0 # for printing progress
        with open('tfidf_{0}.tsv'.format(i+1), 'w') as fout:
            for doc in alldocs:
                fout.write(doc["_id"])
                for term in termlist:
                    tfidf = float(doc["prob"].get(term,0))*(math.log10(docNum)-math.log10(dfdict[term]))
                    fout.write('\t{0}'.format(tfidf))
                fout.write('\n')
                doc_count += 1
                if doc_count % 2000==0: print doc_count

def main(filepath):
    client = MongoClient('localhost', 27017)
    db = client.HTRC
    dfpaths = ['df_1','df_2','df_3']

    importTF(filepath, db)
    # computeTFIDF(db, dfpaths)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Please provide TF file."
    else:
        main(sys.argv[1])
