#!/usr/bin/env python

"""
Siyuan Guo, Apr 2014

This module prepare data for analysis.
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

from pymongo import MongoClient
import pandas as pd

class Data(object):
	"""
	Prepare data for analysis. Convert relevant data stored in MongoDB into a 
	pandas dataframe.
	"""


	def __init__(self):
		# Connect to mongo
		client = MongoClient('localhost', 27017)
		db = client.HTRC
		# Check status of mongoDB
		collections = db.collection_names()
		musthave = ['date', 'nllr_1', 'nllr_2', 'nllr_3']
		missing = set(musthave) - set(collections)
		if missing:
			raise IOError("Collections '%s' doesn't exist in 'HTRC' database. \
				Task aborted." % '&'.join(missing))
		# Initialize, note that db connections don't have getter & setter
		self.datec = db.date
		self.nllrcs = [db.nllr_1, db.nllr_2, db.nllr_3]
		self.data = self.init_data()


	def get_data(self):
		"""Getter for data"""
		return self.data


	def set_data(self, data):
		"""Setter for data"""
		self.data = data


	def init_data(self):
		""" 
		Retrieve identifier('_id') and dependent variable('range') into a pandas 
		dataframe. For now, we only use those documents having date distributions.
		"""
		docs = list(self.datec.find({u"distribution":{"$exists":1}}, 
					{u"firstraw":0, "raw":0, "firstrange":0, "distribution":0}))
		return pd.DataFrame(docs)


	def add_date_features(self):
		"""
		Retrieve and append date features, including first-date-in-text and 
		date-distribution-in-text.
		"""
		docs = list(self.datec.find({u"distribution":{"$exists":1}}, 
								 {u"firstraw":0, "raw":0, "range":0}))
		# The list comprehesion below flattens subdocument 'distribution' out 
		# into root level
		data = pd.DataFrame(
			[dict(doc.pop('distribution'), **doc) for doc in docs]
			)
		data = data.fillna(0.0)
		# Change string reprs of multiclass labels to multiple boolean dummy variables
		dummyvars = pd.DataFrame(
			[{label+'-1st':True} for label in data['firstrange']]
			)
		dummyvars = dummyvars.fillna(False)
		# Replace 'firstrange' column by dummy variables
		data = data.drop('firstrange', 1)
		for colname in dummyvars.columns.values:
			data[colname] = dummyvars[colname]
		# Merge date features with existing data
		data = self.get_data().merge(data, how='inner', on="_id")
		self.set_data(data)


	def add_nllr_features(self):
		"""
		Retrieve and append TE-weighted-normalized-log-likelihood-ratio features
		"""
		data = self.get_data()
		for i in range(3):
			nllrc = self.nllrcs[i]
			nllr = pd.DataFrame.from_dict(
				{doc.pop("_id"):doc for doc in nllrc.find({})}, orient='index'
				)
			nllr.rename(columns=lambda x: x+'-nllr-'+str(i), inplace=True)
			# Inner join data and nllr
			data = data.merge(nllr, how='inner', left_on="_id", right_index=True)
		self.set_data(data)
	
	
	