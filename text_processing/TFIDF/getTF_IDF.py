#! /usr/bin/env python

# calculate TFIDF of a term and import result in MongoDB.
# The collection name of extracted dependent variable is "tf".
# To run: python getTF_IDF.py

# Created by Bin Dai, Mar 2014.

from __future__ import division
from pymongo import MongoClient
from bson.code import Code
import os,re,sys,math

def mapreduce(db):
    map = Code("""function(){
    emit(this._doc_id, this.TF);
    }""")
    reducer = Code(""" function(key,values){
    var total = 0;
    for(var i = 0; i < values.length; i++){
    total += values[i];}
    return total;
    }""")
    db.tf.map_reduce(map,reducer,out ="total_T_1",full_response=True,query={"_doc_id":{"$exists": "true"}})

def aggregation(db):
    total = db.tf.aggregate({$group:{_id:"$_doc_id",total:{$sum:"$TF"}}})
    for doc in total["result"]:
        db.total_T_1.insert(doc)

def main():
    client = MongoClient('localhost', 27017)
    db = client.HTRC
    collections = db.collection_names()
    mapreduce(db)
    doc_Num = db.total_T_1.find().count()
    for i in db.tf.find():
        tf = i["TF"]
        docID = i["_doc_id"]
        total = db.total_T_1.findOne({"_id" : docID}).value
        tc = db.tf.find({"term":i["term"]}).count()
        TF = tf/total
        IDF = math.log10(doc_Num)-math.log10(tc)
        TFIDF = TF*IDF
        db.tf.update({"_doc_id": docID},{"$set":{"TFIDF": TFIDF}})        

if __name__ == '__main__':
    main()
