#! /usr/bin/env python

# Deprecated! HT metadata is no longer used!!!! 
# We moved to HTRC metadata! Run getDV_HTRC.py instead!!!!

# Generate dependent variable in MongoDB.
# The collection name of metadata source is assumed to be "metadata".
# The collection name of extracted dependent variable is "date".
# To run: python getDV.py path/to/metadata.xml

# Created by Siyuan Guo, Feb 2014.

from pymongo import MongoClient
import xml2json, re, sys, json

def xml2mongo(filename, db):
	# insert xml metadata into mongoDB
	try:
		with open(filename) as fin:
			jout = xml2json.xml2json(fin.read())
		    # remove unnecessary enclosing tags, and convert into mongoDB default format
		    jout = re.subn(r"(?<=\})\,\s+(?=\{\"\_id)", "\n", jout[24:len(jout)-3])[0]
			for doc in jout.split('\n'):
				db.metadata.insert(json.loads(doc))
		print "Collection 'metadata' is successfully inserted into 'HTRC' database."
	except IOError:
		print "Failed to insert 'metadata' collection into MongoDB."

def generateDV(db):
	# generate collection for dependent variable
	try:
		for doc in db.metadata.find({"language":"eng"}, {"date":1}):
			if "date" in doc:
				date = doc["date"]
				if re.search(r"^\D*(\d{4}).*$", date):
					doc["date"] = re.subn(r"^\D*(\d{4}).*$", r"\1", date)[0]
				else:
					doc["date"] = "ERROR: "+date
			db.date.insert(doc)
		print "Collection 'date' is successfully inserted into 'HTRC' database."
	except IOError:
		print "Failed to insert 'date' collection into MongoDB."

def main(filename):
	client = MongoClient('localhost', 27017)
	db = client.HTRC
	collections = db.collection_names()
	if "date" in collections:
		print "Collection 'date' already exists in 'HTRC' database."
	elif "metadata" in collections:
		print "Collection 'metadata' already exists in 'HTRC' database. Start generating 'date' using existing 'metadata' collection."
		generateDV(db)
	else:
		xml2mongo(filename, db)
		generateDV(db)
	# Test
	print "empty date: ", db.date.find({"date":""}).count();
	print "nonexistent date: ", db.date.find({"date":{"$exists":0}}).count();
	print "empty&nonexistent date: ", db.date.find({"$or":[{"date":""},{"date":{"$exists":0}}]}).count();
	print "erroneous date: ", db.date.find({"date":{"$regex":"^ERROR:"}}).count();
	print "valid date: ", db.date.find({"date":{"$regex":"^\d{4}"}}).count();

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print "Please provide XML metadata filename."
	elif not sys.argv[1].endswith(".xml"):
		print "Invalid XML metadata filename."
	else:
		main(sys.argv[1])