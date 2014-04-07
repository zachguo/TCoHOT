#! /usr/bin/env python

# This module compute TFIDF given MongoDB data and output computed matrix to a tsv file

# To run: python getTFIDFMatrix.py

# Created by Bin Dai, Siyuan Guo, Mar 2014.


from pymongo import MongoClient
from collections import defaultdict
import math

def reshape(dfdict_mongo):
    return {x['_id']:x['df'] for x in dfdict_mongo}

def computeTFIDF(db): # fix this, remove dfpaths dependency
    dfdict = defaultdict(float)
    tfs = [db.tf_1, db.tf_2, db.tf_3]
    dfs = [db.df_1, db.df_2, db.df_3]
    for i in range(len(tfs)):
        alldocs = tfs[i].find()
        docNum = alldocs.count()
        dfdict = reshape(dfs[i])
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
    computeTFIDF(db)

if __name__ == '__main__':
    main()
