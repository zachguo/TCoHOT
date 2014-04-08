#!/usr/bin/env python

"""
Siyuan Guo, Apr 2014

This module holds functions to prepare data for analysis.
No need to run this module, it's imported by model.py
"""

# To run properly, collection 'date' should look like this:
#
#	 {
#	 	"_id" : "loc.ark+=13960=t02z1nt02",
#	 	"distribution" : {
#	 		"1911-1914" : 0.043683589138134596,
#	 		"1877-1887" : 0.07910271546635184,
#	 		"1896-1901" : 0.08146399055489964,
#	 		"pre-1839" : 0.21251475796930341,
#	 		"1923-present" : 0.01770956316410862,
#	 		"1907-1910" : 0.02833530106257379,
#	 		"1888-1895" : 0.06493506493506493,
#	 		"1902-1906" : 0.043683589138134596,
#	 		"1919-1922" : 0.0070838252656434475,
#	 		"1861-1876" : 0.1959858323494687,
#	 		"1915-1918" : 0.0295159386068477,
#	 		"1840-1860" : 0.1959858323494687
#	 	},
#	 	"firstrange" : "pre-1839",
#	 	"firstraw" : 1824,
#	 	"range" : "1919-1922",
#	 	"raw" : "1919"
#	 }

def preparedata():
	""" 
	Convert relevant data stored in MongoDB into a pandas dataframe.
	"""
	from pymongo import MongoClient
	import pandas as pd
	# Connect to mongo
	client = MongoClient('localhost', 27017)
	db = client.HTRC
	missing = set(['date'])-set(db.collection_names())
	if missing:
		raise IOError("Collections '%s' doesn't exist in 'HTRC' database. \
			Task aborted." % '&'.join(missing))

	# Retrieve relevant data into a pandas dataframe, here we only need 
	# _id, range, distribution and firstrange. '_id' is identifier and range 
	# is dependent variable, all others are features. For current feature 
	# sets, we only use those documents having date distributions.
	docs = list(db.date.find({u"distribution":{"$exists":1}}, 
							 {u"firstraw":0, "raw":0}))
	# The list comprehesion below flattens subdocument 'distribution' out 
	# into root level
	data = pd.DataFrame([dict(doc.pop('distribution'), **doc) for doc in docs])
	data = data.fillna(0.0)
	# Change string reprs of multiclass labels to multiple boolean dummy variables
	dummyvars = pd.DataFrame([{label+'-1st':True} for label in data['firstrange']])
	dummyvars = dummyvars.fillna(False)
	# Replace 'firstrange' column by dummy variables
	data = data.drop('firstrange', 1)
	for colname in dummyvars.columns.values:
		data[colname] = dummyvars[colname]
	return data
	