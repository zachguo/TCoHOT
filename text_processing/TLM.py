#! /usr/bin/env python

"""
Extract features based on temporal language model.

Siyuan Guo, Apr 2014
"""

from pymongo import MongoClient
from collections import defaultdict
from math import log, log10, sqrt
from utils import reshape
import pandas as pd



EPSILON = 0.0001
DATERANGES = ["pre-1839", "1840-1860", "1861-1876", "1877-1887", 
			  "1888-1895", "1896-1901", "1902-1906", "1907-1910", 
			  "1911-1914", "1915-1918", "1919-1922", "1923-present"]



class TLM(object):
	"""
	Temporal Language Model.

	Note, it's more like a bag-of-ngrams model than a true generative language 
	model. Each document and chronon is represented as a bag of ngrams.

	@param datec, connection to date collection in HTRC mongo database.
	@param tfc, connection to one of 'tf_1', 'tf_2' and 'tf_3' collections in 
		            HTRC mongo database.
	"""

	def __init__(self, datec, tfc):
		self.datec = datec
		self.tfc = tfc
		self.rtmatrix = pd.DataFrame()
		self.docids = []
		self.generate_rtmatrix()


	def get_rtmatrix(self):
		"""Getter for rtmatrix"""
		return self.rtmatrix


	def set_rtmatrix(self, rtmatrix):
		"""Setter for rtmatrix"""
		self.rtmatrix = rtmatrix


	def get_docids(self):
		"""Getter for docids"""
		return self.docids


	def set_docids(self, docids):
		"""Setter for docids"""
		self.docids = docids


	def generate_rtmatrix(self):
		"""
		Convert term frequencies stored in a MongoDB collection into a term*daterange 
		matrix. Each cell is raw frequency of a term occurring in a date range.
		(Unsmoothed)

		A sample daterange-term matrix output:
		             pre-1839      1840-1860 ...      1919-1922   1923-present  
		        273885.000000  257701.000000 ...  195615.000000  112005.000000  
		0         6532.000000    2463.000000 ...     845.000000    1052.000000  
		0"-03        0.000001       0.000001 ...       0.000001       0.000001    
		0"-04        0.000001       0.000001 ...       0.000001       0.000001
		...
		[281271 rows x 12 columns]
		"""

		print "Generating term * chronon matrix..."
		# read all doc IDs for each date range from mongoDB into a dictionary:
		# {'pre-1839':['loc.ark+=13960=t9h42611g', ...], ...}
		dr_docid_dict = {}
		for daterange in DATERANGES:
			docs = self.datec.find({"range":daterange}, {"raw":0, "range":0})
			dr_docid_dict[daterange] = [doc['_id'] for doc in docs]

		# read term frquencies from mongoDB for each docid, then aggregate tf by 
		# timeslice, aggregated result is stored in a 2D dictionary:
		# {'pre-1839':{u'': 273885.0, u'1,808': 14.0, u'woode': 6.0, ...}, ...}
		dr_tf_dict = {}
		for daterange in DATERANGES:
			dr_tf_dict[daterange] = defaultdict(int)
			docids = dr_docid_dict[daterange]
			for docid in docids:
				tfdoc = self.tfc.find_one({"_id":docid})
				if tfdoc:
					# read raw term frequencies for each docid from mongoDB
					tfdict = tfdoc[u'freq']
					# merge & sum term frequencies for each date range
					for term in tfdict:
						dr_tf_dict[daterange][term] += tfdict[term]
				else:
					print "Warning: no term frequency for doc %s." % docid

		# Convert 2D dictionary into pandas dataframe (named matrix), with a simple
		rtmatrix = pd.DataFrame(dr_tf_dict).fillna(EPSILON)
		# Reorder columns of range * term matrix
		rtmatrix = rtmatrix[DATERANGES]
		self.set_rtmatrix(rtmatrix)
		self.set_docids(reduce(lambda x, y: x+y, dr_docid_dict.values()))



class NLLR(TLM):
	"""
	Temporal Entropy Weighted Normalized Log Likelihood Ratio 
	A document distance metric from Kanhabua & Norvag (2008)
	Lots of lambdas & idiomatic pandas functions will be used, they're superfast!

	@param datec, connection to date collection in HTRC mongo database.
	@param tfc, connection to one of 'tf_1', 'tf_2' and 'tf_3' collections in 
		            HTRC mongo database.
	@param nllrc, connection to one of 'nllr_1', 'nllr_2' and 'nllr_3' collections
					to store NLLR results.
	"""

	def __init__(self, datec, tfc, nllrc):
		TLM.__init__(self, datec, tfc)
		self.nllrc = nllrc


	@staticmethod
	def compute_llr(rtmatrix):
		"""
		Compute log( p(w|dr) / p(w/C) ), where dr is daterange and C is corpora.

		@param rtmatrix, a pandas dataframe representing term * daterange matrix.
		@return a 2D dictionary in format of {'pre-1839':{'term': 0.003, ...}, ...}
		"""

		# Normalize each column from freq to prob: p(w|dr)
		tfdaterange = rtmatrix.div(rtmatrix.sum(axis=0), axis=1)
		# Sum all columns into one column then convert from freq to prob: p(w|C)
		tfcorpora = rtmatrix.sum(axis=1)
		tfcorpora = tfcorpora.div(tfcorpora.sum(axis=0))
		# Compute log likelihood ratio
		llrmatrix = tfdaterange.div(tfcorpora, axis=0).applymap(log)
		return llrmatrix.to_dict()

		
	@staticmethod
	def compute_te(rtmatrix):
		"""
		Compute temporal entropy for each term.

		@param rtmatrix, a pandas dataframe representing term * daterange matrix.
		@return a dictionary of temporal entropies (term as key):
		        {u'murwara': 0.9999989777855017, 
		         u'fawn': 0.8813360127166802,
		         ... }
		"""

		# Normalize each row from freq to prob
		rtmatrix = rtmatrix.div(rtmatrix.sum(axis=1), axis=0)
		# compute temporal entropy and return it, 12 is number of chronons.
		rtmatrix = rtmatrix.applymap(lambda x: x*log(x)).sum(axis=1)
		return rtmatrix.apply(lambda e: 1+1/log(12)*e).to_dict()


	def compute_nllr(self, weighted=True):
		"""
		Compute normalized log likelihood ratio, using deJong/Rode/Hiemstra 
		Temporal Language Model, with a option of weighting by temporal 
		entropy.

		@param weighted, whether or not weighted by temporal entropy.
		@return a 2D dictionary of NLLRs in format {docid:{daterange: .. } .. }
		"""

		print 'Computing TEwNLLR...'
		nllrdict = {}
		llrdict = self.compute_llr(self.get_rtmatrix())
		tedict = self.compute_te(self.get_rtmatrix()) if weighted else {}
		docids = self.get_docids()
		# read p(w|d) from MongoDB ('prob' field in tf_n collections)
		for docid in docids:
			tfdoc = self.tfc.find_one({u"_id":docid})
			if tfdoc:
				probs = tfdoc[u"prob"]
				nllrdict[docid] = {}
				for daterange in DATERANGES:
					nllrdict[docid][daterange] = sum([tedict[term] * probs[term] * llrdict[daterange][term] for term in probs])
		return nllrdict


	def run(self):
		"""Run"""
		self.nllrc.insert(reshape(self.compute_nllr()))



class CS(TLM):
	"""
	Cosine similarity

	@param datec, connection to date collection in HTRC mongo database.
	@param tfc, connection to one of 'tf_1', 'tf_2' and 'tf_3' collections in 
		            HTRC mongo database.
	@param csc, connection to one of 'csc_1', 'csc_2' and 'csc_3' collections
					to store Cos-Sim results.
	"""
	
	def __init__(self, datec, tfc, csc):
		TLM.__init__(self, datec, tfc)
		self.csc = csc


	def compute_cs(self):
		"""
		Compute cosine similarity between each pair of term & chronon

		@return a 2D dictionary of CSs in format {docid:{daterange: .. } .. }
		"""

		print 'Computing Cosine-similarity...'
		csdict = {}
		docids = self.get_docids()
		rtmatrix = self.get_rtmatrix()
		# Normalize each column from freq to prob: p(w|dr)
		rtmatrix = rtmatrix.div(rtmatrix.sum(axis=0), axis=1)
		# a vector of which each cell is the vector length for a chronon
		rvlength = rtmatrix.applymap(lambda x: x*x).sum(axis=0).apply(sqrt)
		rvlength = rvlength.to_dict()
		rtmatrix = rtmatrix.to_dict()
		for docid in docids:
			tfdoc = self.tfc.find_one({u"_id":docid})
			if tfdoc:
				probs = tfdoc[u"prob"]
				csdict[docid] = {}
				# a vector of which each cell is the vector length for a doc
				dvlength = sqrt(sum([x*x for x in probs.values()]))
				for daterange in DATERANGES:
					cossim = sum([probs[term] * rtmatrix[daterange][term] for term in probs]) / (dvlength * rvlength[daterange])
					csdict[docid][daterange] = cossim if cossim >= -1 and cossim <= 1 else 0
		return csdict


	def run(self):
		"""Run"""
		self.csc.insert(reshape(self.compute_cs()))



class KLD(TLM):
	"""
	Kullback-Leibler divergence

	@param datec, connection to date collection in HTRC mongo database.
	@param tfc, connection to one of 'tf_1', 'tf_2' and 'tf_3' collections in 
		            HTRC mongo database.
	@param kldc, connection to one of 'kld_1', 'kld_2' and 'kld_3' collections
					to store KL Divergence results.
	"""

	def __init__(self, datec, tfc, kldc):
		TLM.__init__(self, datec, tfc)
		self.kldc = kldc


	def compute_kld(self):
		"""
		Compute KL-Divergence for each pair of term & daterange

		@return a 2D dictionary of KLDs in format {docid:{daterange: .. } .. }
		"""

		print 'Computing KL-Divergence...'
		klddict = {}
		docids = self.get_docids()
		rtmatrix = self.get_rtmatrix()
		# Normalize each column from freq to prob: p(w|dr)
		rtmatrix = rtmatrix.div(rtmatrix.sum(axis=0), axis=1).to_dict()
		for docid in docids:
			tfdoc = self.tfc.find_one({u"_id":docid})
			if tfdoc:
				probs = tfdoc[u"prob"]
				klddict[docid] = {}
				for daterange in DATERANGES:
					klddict[docid][daterange] = sum([probs[term] * log10(probs[term]/rtmatrix[daterange][term]) for term in probs])
		return klddict


	def run(self):
		"""Run"""
		self.kldc.insert(reshape(self.compute_kld()))



class RunTLM(object):
	"""
	Run various computation based on TLM and save results to mongoDB.
	Collections 'date','tf_1','tf_2','tf_3','cf' must exist in mongoDB 
	before execution.

	@param outcollections, a list of names of output collections.
	"""

	def __init__(self, outcollections):
		db = self.connect_mongo(outcollections)
		self.datec = db.date
		self.tfcs = [db.tf_1, db.tf_2, db.tf_3]
		self.cfc = db.cf
		self.outcs = [db[outc] for outc in outcollections]


	@staticmethod
	def connect_mongo(outcollections):
		"""
		Connect to mongo, and check collection status.

		@param outcollections, a list of names of output collections.
		@return db connection.
		"""
		client = MongoClient('localhost', 27017)
		db = client.HTRC
		collections = db.collection_names()
		musthave = ['date', 'tf_1', 'tf_2', 'tf_3', 'cf']
		missing = set(musthave) - set(collections)
		if missing:
			raise IOError("Collections '%s' doesn't exist in 'HTRC' database. \
				Task aborted." % '&'.join(missing))
		for clc in outcollections:
			if clc in collections: 
				print "Collection %s already exists in 'HTRC' database. Drop it." % clc
				db.drop_collection(clc)
		return db


	def run_nllr(self):
		"""Run NLLR"""
		print 'Generate NLLR...'
		for outc, tfc in zip(self.outcs, self.tfcs):
			NLLR(self.datec, tfc, outc).run()


	def run_kld(self):
		"""Run KLD"""
		print 'Generate KLD...'
		for outc, tfc in zip(self.outcs, self.tfcs):
			KLD(self.datec, tfc, outc).run()


	def run_cs(self):
		"""Run CS"""
		print 'Generate CS...'
		for outc, tfc in zip(self.outcs, self.tfcs):
			CS(self.datec, tfc, outc).run()


	def run_ocr(self):
		"""Run OCR (NLLR based on character language model)"""
		print 'Generate OCR...'
		NLLR(self.datec, self.cfc, self.outcs[0]).run()



# Feature extraction jobs
def job1(): RunTLM(['nllr_1', 'nllr_2', 'nllr_3']).run_nllr()
def job2(): RunTLM(['kld_1', 'kld_2', 'kld_3']).run_kld()
def job3(): RunTLM(['cs_1', 'cs_2', 'cs_3']).run_cs()
def job4(): RunTLM(['nllr_ocr']).run_ocr()


def run_parallel():
	"""Run jobs in parallel, may need at least 8gb memory"""
	from multiprocessing import Pool
	pool = Pool()
	result1 = pool.apply_async(job1, [])
	result2 = pool.apply_async(job2, [])
	result3 = pool.apply_async(job3, [])
	result4 = pool.apply_async(job4, [])
	result1.get()
	result2.get() 
	result3.get()
	result4.get()


def run_serial():
	"""Run jobs in serial, 2gb memory should be enough"""
	job1()
	job2()
	job3()
	job4()



if __name__ == '__main__':
	# run_serial()
	run_parallel()
