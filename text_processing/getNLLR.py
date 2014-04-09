#! /usr/bin/env python

"""
Created by Siyuan Guo, Mar 2014

This script generate ...
Collections 'date','tf_1','tf_2','tf_3' must exist in mongoDB before execution.
Lots of lambdas & idiomatic pandas functions will be used, they're superfast!
"""

from pymongo import MongoClient
from collections import defaultdict
import pandas, math


DATERANGES = ["pre-1839", "1840-1860", "1861-1876", "1877-1887", "1888-1895", 
			  "1896-1901", "1902-1906", "1907-1910", "1911-1914", "1915-1918", 
			  "1919-1922", "1923-present"]
EPSILON = 0.000001 # constant for smoothing


def get_dtmatrix(datec, tfc):
	"""
	Convert term frequencies stored in a MongoDB collection into a term * daterange 
	matrix. 

	@param datec, connection to date collection in HTRC mongo database.
	@param tfc, connection to one of db.tf_1, db.tf_2 and db.tf_3 collections in 
	            HTRC mongo database.

	A sample term-timeslice matrix output:
	             pre-1839      1840-1860 ...      1919-1922   1923-present  
	        273885.000000  257701.000000 ...  195615.000000  112005.000000  
	0         6532.000000    2463.000000 ...     845.000000    1052.000000  
	0"-03        0.000001       0.000001 ...       0.000001       0.000001    
	0"-04        0.000001       0.000001 ...       0.000001       0.000001
	...
	[281271 rows x 12 columns]
	"""
	# read all doc IDs for each date range from mongoDB into a dictionary:
	# {'pre-1839':['loc.ark+=13960=t9h42611g', ...], ...}
	dr_docid_dict = {}
	for daterange in DATERANGES:
		docs = datec.find({"range":daterange}, {"raw":0, "range":0})
		dr_docid_dict[daterange] = [doc['_id'] for doc in docs]

	# read term frquencies from mongoDB for each doc_id, then aggregate tf by 
	# timeslice, aggregated result is stored in a 2D dictionary:
	# {'pre-1839':{u'': 273885.0, u'1,808': 14.0, u'woode': 6.0, ...}, ...}
	dr_tf_dict = {} 
	for daterange in DATERANGES:
		dr_tf_dict[daterange] = defaultdict(int)
		docids = dr_docid_dict[daterange]
		for doc_id in docids:
			try:
				# read raw term frequencies for each doc_id from mongoDB
				tfdict = tfc.find_one({"_id":doc_id})['freq']
				# merge & sum term frequencies for each date range
				for term in tfdict:
					dr_tf_dict[daterange][term] += tfdict[term]
			except:
				print "No term frequency for doc %s." % doc_id

	# Convert 2D dictionary into pandas dataframe (named matrix), with a simple
	# smoothing: add EPSILON.
	dtmatrix = pandas.DataFrame(dr_tf_dict).fillna(EPSILON)
	# Reorder columns
	dtmatrix = dtmatrix[DATERANGES]
	# # Optional: Filter terms(rows) with too small frequency (less than 50)
	# dtmatrix = dtmatrix[dtmatrix.apply(lambda x:sum(x)>=50, axis=1)]
	return dtmatrix


def get_log_likelihood_ratio(dtmatrix):
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
	llrmatrix = llrmatrix.applymap(math.log)
	return llrmatrix.to_dict()


def get_normalized_log_likelihood_ration(dtmatrix, tfc):
	"""
	Compute NLLR, using deJong/Rode/Hiemstra Temporal Language Model.
	"""
	nllrdict = {}
	llrdict = get_log_likelihood_ratio(dtmatrix)
	# read p(w|d) from MongoDB ('prob' field in tf_n collections)
	for doc in tfc.find({}, {"freq":0}):
		doc_id = doc[u"_id"]
		probs = doc[u"prob"]
		nllrdict[doc_id] = {}
		for daterange in llrdict:
			nllrdict[doc_id][daterange] = sum([probs[term] * llrdict[daterange].get(term, 0) for term in probs])
	return nllrdict

		
def get_temporal_entropy(dtmatrix):
	"""
	Compute temporal entropy for each term.
	@param dtmatrix, a pandas dataframe representing term * daterange matrix.
	@return a dictionary of temporal entropies (term as key):
	        {u'murwara': 7.4756799084057523e-06, 
	         u'fawn': 0.86781589101379875,
	         ... }
	"""
	# Normalize each row from freq to prob
	dtmatrix = dtmatrix.div(dtmatrix.sum(axis=1), axis=0)
	# compute temporal entropy and return it
	return dtmatrix.applymap(lambda x: -x*math.log(x)).sum(axis=1).to_dict()


def main():
	"""Run"""
	client = MongoClient('localhost', 27017)
	db = client.HTRC
	missing = set(['date', 'tf_1', 'tf_2', 'tf_3']) - set(db.collection_names())
	if missing:
		raise IOError("Collections '%s' doesn't exist in 'HTRC' database. \
			Task aborted." % '&'.join(missing))
	for nllrc, tfc in zip([db.nllr_1, db.nllr_2, db.nllr_3], [db.tf_1, db.tf_2, db.tf_3]):
		dtmatrix = get_dtmatrix(db.date, tfc)
		nllrdict = get_normalized_log_likelihood_ration(dtmatrix, tfc)
		# transform and save computed NLLR into mongoDB
		nllrdict = [dict(nllrdict[d], **{u"_id":d}) for d in nllrdict]
		nllrc.insert(nllrdict)


if __name__ == '__main__':
	main()
