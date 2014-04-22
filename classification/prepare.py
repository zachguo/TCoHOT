#!/usr/bin/env python

"""
Prepare data for analysis.
Don't run this module, it's imported by model.py

Siyuan Guo, Apr 2014
"""


from pymongo import MongoClient
import pandas as pd


class Data(object):
	"""
	Prepare data for analysis. Convert relevant data stored in MongoDB into a 
	pandas dataframe.
	"""


	def __init__(self):
		self.db = self.connect_mongo()
		self.datec = self.db.date
		self.data = self.init_data()


	@staticmethod
	def connect_mongo():
		"""
		Connect to mongo, and check collection status.
		"""
		client = MongoClient('localhost', 27017)
		db = client.HTRC
		# Check status of mongoDB
		collections = db.collection_names()
		musthave = ['date', 'nllr_1', 'nllr_2', 'nllr_3', 'kld_1', 'kld_2', 'kld_3']
		missing = set(musthave) - set(collections)
		if missing:
			raise IOError("Collections '%s' doesn't exist in 'HTRC' database. \
				Task aborted." % '&'.join(missing))
		return db


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


	def add_text_features(self, featurecnames):
		"""
		Retrieve and append text-related features

		@param featurecnames, a list of names of text-related feature collections
		"""
		data = self.get_data()
		for featurecname in featurecnames:
			featurec = self.db[featurecname]
			feature = pd.DataFrame.from_dict(
				{doc.pop("_id"):doc for doc in featurec.find({})}, orient='index'
				)
			feature.rename(columns=lambda x: x+'-'+featurecname, inplace=True)
			# Inner join data and feature
			data = data.merge(feature, how='inner', left_on="_id", right_index=True)
		self.set_data(data)


	def add_nllr_features(self):
		"""
		Retrieve and append TE-weighted-normalized-log-likelihood-ratio features
		"""
		self.add_text_features(['nllr_1', 'nllr_2', 'nllr_3'])
		print 'Add TE-weighted-normalized-log-likelihood-ratio features.'
	
	
	def add_kld_features(self):
		"""
		Retrieve and append KL-divergence features
		"""
		self.add_text_features(['kld_1', 'kld_2', 'kld_3'])
		print 'Add KL-divergence features.'


	def add_cs_features(self):
		"""
		Retrieve and append Cosine-Similarity features
		"""
		self.add_text_features(['cs_1', 'cs_2', 'cs_3'])
		print 'Add Cosine-Similarity features.'


	def add_ocr_features(self):
		"""
		Retrieve and append OCR features
		"""
		self.add_text_features(['nllr_ocr'])
		print 'Add OCR features.'
