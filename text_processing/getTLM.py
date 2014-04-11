#! /usr/bin/env python

"""
Created by Siyuan Guo, Mar 2014
"""

from pymongo import MongoClient
from collections import defaultdict
from math import log
import pandas as pd



class TLM(object):
	"""
	Temporal Language Model

	@param datec, connection to date collection in HTRC mongo database.
	@param tfc, connection to one of 'tf_1', 'tf_2' and 'tf_3' collections in 
		            HTRC mongo database.
	"""

	def __init__(self, datec, tfc):
		self.datec = datec
		self.tfc = tfc
		self.dtmatrix = pd.DataFrame()
		self.compute_dtmatrix()


	def get_dtmatrix(self):
		"""Getter for dtmatrix"""
		return self.dtmatrix


	def set_dtmatrix(self, dtmatrix):
		"""Setter for dtmatrix"""
		self.dtmatrix = dtmatrix


	def compute_dtmatrix(self):
		"""
		Convert term frequencies stored in a MongoDB collection into a term*daterange 
		matrix. 

		A sample daterange-term matrix output:
		             pre-1839      1840-1860 ...      1919-1922   1923-present  
		        273885.000000  257701.000000 ...  195615.000000  112005.000000  
		0         6532.000000    2463.000000 ...     845.000000    1052.000000  
		0"-03        0.000001       0.000001 ...       0.000001       0.000001    
		0"-04        0.000001       0.000001 ...       0.000001       0.000001
		...
		[281271 rows x 12 columns]
		"""

		EPSILON = 0.000001 # constant for smoothing
		DATERANGES = ["pre-1839", "1840-1860", "1861-1876", "1877-1887", 
					  "1888-1895", "1896-1901", "1902-1906", "1907-1910", 
					  "1911-1914", "1915-1918", "1919-1922", "1923-present"]

		# read all doc IDs for each date range from mongoDB into a dictionary:
		# {'pre-1839':['loc.ark+=13960=t9h42611g', ...], ...}
		dr_docid_dict = {}
		for daterange in DATERANGES:
			docs = self.datec.find({"range":daterange}, {"raw":0, "range":0})
			dr_docid_dict[daterange] = [doc['_id'] for doc in docs]

		# read term frquencies from mongoDB for each doc_id, then aggregate tf by 
		# timeslice, aggregated result is stored in a 2D dictionary:
		# {'pre-1839':{u'': 273885.0, u'1,808': 14.0, u'woode': 6.0, ...}, ...}
		dr_tf_dict = {} 
		for daterange in DATERANGES:
			dr_tf_dict[daterange] = defaultdict(int)
			docids = dr_docid_dict[daterange]
			for doc_id in docids:
				tfdoc = self.tfc.find_one({"_id":doc_id})
				if tfdoc:
					# read raw term frequencies for each doc_id from mongoDB
					tfdict = tfdoc['freq']
					# merge & sum term frequencies for each date range
					for term in tfdict:
						dr_tf_dict[daterange][term] += tfdict[term]
				else:
					print "No term frequency for doc %s." % doc_id

		# Convert 2D dictionary into pandas dataframe (named matrix), with a simple
		# smoothing: add EPSILON.
		dtmatrix = pd.DataFrame(dr_tf_dict).fillna(EPSILON)
		# Reorder columns
		dtmatrix = dtmatrix[DATERANGES]
		# # Optional: Filter terms(rows) with too small frequency (less than 50)
		# dtmatrix = dtmatrix[dtmatrix.apply(lambda x:sum(x)>=50, axis=1)]
		self.set_dtmatrix(dtmatrix)



class NLLR(TLM):
	"""
	Temporal Entropy Weighted Normalized Log Likelihood Ratio

	@param datec, connection to date collection in HTRC mongo database.
	@param tfc, connection to one of 'tf_1', 'tf_2' and 'tf_3' collections in 
		            HTRC mongo database.
	@param nllrc, connection to one of 'nllr_1', 'nllr_2' and 'nllr_3' collections
					to store NLLR results.
	Lots of lambdas & idiomatic pandas functions will be used, they're superfast!
	"""

	def __init__(self, datec, tfc, nllrc):
		TLM.__init__(self, datec, tfc)
		self.nllrc = nllrc


	@staticmethod
	def compute_llr(dtmatrix):
		"""
		Compute log( p(w|dr) / p(w/C) ), where dr is daterange and C is corpora.

		@param dtmatrix, a pandas dataframe representing term * daterange matrix.
		@return a 2D dictionary in format of {'pre-1839':{'term': 0.003, ...}, ...}
		"""
		# Normalize each column from freq to prob: p(w|dr)
		tfdaterange = dtmatrix.div(dtmatrix.sum(axis=0), axis=1)
		# Sum all columns into one column then convert from freq to prob: p(w|C)
		tfcorpora = dtmatrix.sum(axis=1)
		tfcorpora = tfcorpora.div(tfcorpora.sum(axis=0))
		# Compute log likelihood ratio
		llrmatrix = tfdaterange.div(tfcorpora, axis=0)
		llrmatrix = llrmatrix.applymap(log)
		return llrmatrix.to_dict()

		
	@staticmethod
	def compute_te(dtmatrix):
		"""
		Compute temporal entropy for each term.

		@param dtmatrix, a pandas dataframe representing term * daterange matrix.
		@return a dictionary of temporal entropies (term as key):
		        {u'murwara': 0.9999989777855017, 
		         u'fawn': 0.8813360127166802,
		         ... }
		"""
		# Normalize each row from freq to prob
		dtmatrix = dtmatrix.div(dtmatrix.sum(axis=1), axis=0)
		# compute temporal entropy and return it
		# note that our formula is a bit different from that in (N&K 2005)
		# instead of normalizing temporal entropy by 1/log(N), we simply use
		# 1/log(1500) because our number of documents in each chronon all 
		# around 1500.
		dtmatrix = dtmatrix.applymap(lambda x: x*log(x)).sum(axis=1)
		dtmatrix = dtmatrix.apply(lambda e: 1+1/log(1500)*e)
		return dtmatrix.to_dict()


	def compute_nllr(self, weighted=True):
		"""
		Compute normalized log likelihood ratio, using deJong/Rode/Hiemstra 
		Temporal Language Model, with a option of weighting by temporal 
		entropy.

		@param weighted, whether or not weighted by temporal entropy.
		"""
		nllrdict = {}
		llrdict = self.compute_llr(self.get_dtmatrix())
		tedict = self.compute_te(self.get_dtmatrix()) if weighted else {}
		# read p(w|d) from MongoDB ('prob' field in tf_n collections)
		for doc in self.tfc.find({}, {"freq":0}):
			doc_id = doc[u"_id"]
			probs = doc[u"prob"]
			nllrdict[doc_id] = {}
			for daterange in llrdict:
				# note not all terms of probs exist in llrdict & tedict
				nllrdict[doc_id][daterange] = sum([tedict.get(term, 1) * probs[term] * llrdict[daterange].get(term, 0) for term in probs])
		return nllrdict


	def run(self):
		"""Run"""
		nllrdict = self.compute_nllr()
		# transform and save computed NLLR into mongoDB
		nllrdict = [dict(nllrdict[d], **{u"_id":d}) for d in nllrdict]
		self.nllrc.insert(nllrdict)



class RunNLLRs(object):
	"""
	Run NLLR computation for unigram, bigram and trigram.
	Collections 'date','tf_1','tf_2','tf_3' must exist in mongoDB before execution.
	"""

	def __init__(self):
		db = self.connect_mongo()
		self.datec = db.date
		self.tfcs = [db.tf_1, db.tf_2, db.tf_3]
		self.nllrcs = [db.nllr_1, db.nllr_2, db.nllr_3]


	@staticmethod
	def connect_mongo():
		"""
		Connect to mongo, and check collection status.
		"""
		client = MongoClient('localhost', 27017)
		db = client.HTRC
		collections = db.collection_names()
		musthave = ['date', 'tf_1', 'tf_2', 'tf_3']
		missing = set(musthave) - set(collections)
		if missing:
			raise IOError("Collections '%s' doesn't exist in 'HTRC' database. \
				Task aborted." % '&'.join(missing))
		for clc in ['nllr_1', 'nllr_2', 'nllr_3']:
			if clc in collections: 
				print "Collection %s already exists in 'HTRC' database. Drop it." % clc
				db.drop_collection(clc)
		return db


	def run(self):
		"""Run"""
		for nllrc, tfc in zip(self.nllrcs, self.tfcs):
			NLLR(self.datec, tfc, nllrc).run()



if __name__ == '__main__':
	RunNLLRs().run()
