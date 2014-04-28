#! /usr/bin/env python

"""
Extract features based on temporal language model.

Siyuan Guo, Apr 2014
"""

from pymongo import MongoClient
from collections import defaultdict
from itertools import groupby
from math import log, log10, sqrt
from utils import reshape, fakedict
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
	@param weighted, whether or not weighted by temporal entropy.
	"""

	def __init__(self, datec, tfc, weighted=True):
		self.datec = datec
		self.tfc = tfc
		self.rtmatrix = pd.DataFrame()
		self.docids = []
		self.generate_rtmatrix()
		self.tedict = self.compute_te(self.get_rtmatrix()) if weighted else fakedict()


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
				# else:
				# 	print "Warning: no term frequency for doc %s." % docid

		# Convert 2D dictionary into pandas dataframe (named matrix), with a simple
		rtmatrix = pd.DataFrame(dr_tf_dict).fillna(EPSILON)
		# Reorder columns of range * term matrix
		rtmatrix = rtmatrix[DATERANGES]
		self.set_rtmatrix(rtmatrix)
		self.set_docids(reduce(lambda x, y: x+y, dr_docid_dict.values()))


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

		print "Generating temporal entropy..."
		# Normalize each row from freq to prob
		rtmatrix = rtmatrix.div(rtmatrix.sum(axis=1), axis=0)
		# compute temporal entropy and return it, 12 is number of chronons.
		rtmatrix = rtmatrix.applymap(lambda x: x*log(x)).sum(axis=1)
		return rtmatrix.apply(lambda e: 1+1/log(12)*e).to_dict()

	def output_rtmatrix(self, filename):
		"""
		Output term * daterange matrix with TE into a csv file.
		"""
		self.get_rtmatrix().merge(pd.DataFrame({'TE':self.tedict}), left_index=True, right_index=True).to_csv(filename)

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


	def compute_nllr(self, nllrc):
		"""
		Compute Temporal Entropy Weighted Normalized Log Likelihood Ratio, 
		a document distance metric from Kanhabua & Norvag (2008) using 
		deJong/Rode/Hiemstra Temporal Language Model.
		Lots of lambdas & idiomatic pandas functions will be used.

		@param nllrc, connection to nllr output collection, one of 'nllr_1', 'nllr_2', 
		             'nllr_3' and 'nllr_ocr'.
		"""

		print 'Computing TEwNLLR...'
		count = 0
		nllrdict = {} # a 2D dictionary of CSs in format {docid:{daterange: .. } .. }
		llrdict = self.compute_llr(self.get_rtmatrix())
		# read p(w|d) from MongoDB ('prob' field in tf_n collections)
		for docid in self.get_docids():
			tfdoc = self.tfc.find_one({u"_id":docid})
			if tfdoc:
				probs = tfdoc[u"prob"]
				nllrdict[docid] = {}
				for daterange in DATERANGES:
					nllrdict[docid][daterange] = sum([self.tedict[term] * probs[term] * llrdict[daterange][term] for term in probs])
				count += 1
				if count % 10000 == 0: 
					print '  Finish computing NLLR for %s docs.' % count
					nllrc.insert(reshape(nllrdict))
					nllrdict = {}
		# don't forget leftover nllrdict
		print '  Finish computing NLLR for %s docs.' % count
		nllrc.insert(reshape(nllrdict))


	def compute_cs(self, csc):
		"""
		Compute cosine similarity between each pair of term & chronon

		@param csc, connection to cs output collection, one of 'cs_1', 'cs_2', 
		             'cs_3' and 'cs_ocr'.
		"""

		print 'Computing Cosine-similarity...'
		count = 0
		csdict = {} # a 2D dictionary of CSs in format {docid:{daterange: .. } .. }
		rtmatrix = self.get_rtmatrix()
		# Normalize each column from freq to prob: p(w|dr)
		rtmatrix = rtmatrix.div(rtmatrix.sum(axis=0), axis=1)
		# a vector of which each cell is the vector length for a chronon
		rvlength = rtmatrix.applymap(lambda x: x*x).sum(axis=0).apply(sqrt)
		rvlength = rvlength.to_dict()
		rtmatrix = rtmatrix.to_dict()
		for docid in self.get_docids():
			tfdoc = self.tfc.find_one({u"_id":docid})
			if tfdoc:
				probs = tfdoc[u"prob"]
				csdict[docid] = {}
				# a vector of which each cell is the vector length for a doc
				dvlength = sqrt(sum([x*x for x in probs.values()]))
				for daterange in DATERANGES:
					cossim = sum([self.tedict[term] * probs[term] * rtmatrix[daterange][term] for term in probs]) / (dvlength * rvlength[daterange])
					csdict[docid][daterange] = cossim if cossim >= -1 and cossim <= 1 else 0
				count += 1
				if count % 10000 == 0: 
					print '  Finish computing CS for %s docs.' % count
					csc.insert(reshape(csdict))
					csdict = {}
		# don't forget leftover csdict
		print '  Finish computing CS for %s docs.' % count
		csc.insert(reshape(csdict))


	def compute_kld(self, kldc):
		"""
		Compute KL-Divergence for each pair of term & daterange

		@param kldc, connection to kld output collection, one of 'kld_1', 'kld_2', 
		             'kld_3' and 'kld_ocr'.
		"""

		print 'Computing KL-Divergence...'
		count = 0
		klddict = {} # a 2D dictionary of KLDs in format {docid:{daterange: .. } .. }
		rtmatrix = self.get_rtmatrix()
		# Normalize each column from freq to prob: p(w|dr)
		rtmatrix = rtmatrix.div(rtmatrix.sum(axis=0), axis=1).to_dict()
		for docid in self.get_docids():
			tfdoc = self.tfc.find_one({u"_id":docid})
			if tfdoc:
				probs = tfdoc[u"prob"]
				klddict[docid] = {}
				for daterange in DATERANGES:
					klddict[docid][daterange] = sum([self.tedict[term] * probs[term] * log10(probs[term]/rtmatrix[daterange][term]) for term in probs])
				count += 1
				if count % 10000 == 0: 
					print '  Finish computing KLD for %s docs.' % count
					kldc.insert(reshape(klddict))
					klddict = {}
		# don't forget leftover klddict
		print '  Finish computing KLD for %s docs.' % count
		kldc.insert(reshape(klddict))


	def run(self, outc):
		"""
		Run

		@param outc, connection to output collection, one of 'nllr_1', 'nllr_2', 
		             'nllr_3', 'nllr_ocr', 'csc_1', 'csc_2', 'csc_3', 'csc_ocr', 
		             'kld_1', 'kld_2', 'kld_3' and 'kld_ocr'.
		"""
		metric, _, postfix = outc.name.rpartition('_')
		if postfix != self.tfc.name.rpartition('_')[-1]:
			raise ValueError('Names of tf and output collection do not match')
		if metric == 'nllr':
			self.compute_nllr(outc)
		elif metric == 'cs':
			self.compute_cs(outc)
		elif metric == 'kld':
			self.compute_kld(outc)
		else:
			raise ValueError('Invalid metric.')



class RunTLM(object):
	"""
	Run various computation based on TLM and save results to mongoDB.
	Collections 'date','tf_1','tf_2','tf_3','tf_ocr' must exist in mongoDB 
	before execution.

	@param outcollections, a list of names of output collections.
	"""

	def __init__(self, outcollections):
		db, outcs = self.connect_mongo(outcollections)
		self.datec = db.date
		self.tfcs = [db.tf_1, db.tf_2, db.tf_3, db.tf_ocr]
		self.outcs = [db[outc] for outc in outcs] if outcs else []


	@staticmethod
	def connect_mongo(outcollections):
		"""
		Connect to mongo, and check collection status.

		@param outcollections, a list of names of output collections.
		@return db, connection to database.
		@return outcs, names of output collections that don't exist.
		"""
		client = MongoClient('localhost', 27017)
		db = client.HTRC
		collections = db.collection_names()
		musthave = ['date', 'tf_1', 'tf_2', 'tf_3', 'tf_ocr']
		missing = set(musthave) - set(collections)
		if missing:
			raise IOError("Collections '%s' doesn't exist in 'HTRC' database. \
				Task aborted." % '&'.join(missing))
		outcs = []
		for clc in outcollections:
			if clc in collections: 
				print "Collection %s already exists in 'HTRC' database, skip." % clc
			else:
				outcs.append(clc)
		return db, outcs


	def output_rtmatrixes(self):
		"""Output rtmatrixes into csvs"""
		for i in range(len(self.tfcs)):
			TLM(self.datec, self.tfcs[i]).output_rtmatrix('rtmatrix_%s.csv' % i)


	def run(self, weighted=True):
		"""
		Run.
		"""
		if self.outcs:
			# group by postfix ('_1', '_2', '_3' & '_ocr')
			for gtuple in groupby(self.outcs, lambda x: x.name.rpartition('_')[-1]):
				postfix, outcs = gtuple
				outcs = list(outcs)
				if postfix == '1':
					model = TLM(self.datec, self.tfcs[0], weighted)
				elif postfix == '2':
					model = TLM(self.datec, self.tfcs[1], weighted)
				elif postfix == '3':
					model = TLM(self.datec, self.tfcs[2], weighted)
				elif postfix == 'ocr':
					model = TLM(self.datec, self.tfcs[-1], weighted)
				else:
					raise ValueError('Invalid output collection names.')
				for outc in outcs:
					print 'Generate %s...' % outc.name
					model.run(outc)
		else:
			print 'All output collections already exists.'



# Feature extraction jobs
WEIGHTED = True
def job1(): RunTLM(['nllr_1', 'kld_1', 'cs_1']).run(WEIGHTED)
def job2(): RunTLM(['nllr_2', 'kld_2', 'cs_2']).run(WEIGHTED)
def job3(): RunTLM(['nllr_3', 'kld_3', 'cs_3']).run(WEIGHTED)
def job4(): RunTLM(['nllr_ocr', 'kld_ocr', 'cs_ocr']).run(WEIGHTED)

def run_parallel():
	"""Run jobs in parallel, may need at least 16gb memory"""
	from multiprocessing import Pool
	pool = Pool(processes=2)
	result1 = pool.apply_async(job1, [])
	result2 = pool.apply_async(job2, [])
	result3 = pool.apply_async(job3, [])
	result4 = pool.apply_async(job4, [])
	result1.get()
	result2.get()
	result3.get()
	result4.get()

def run_serial():
	"""Run jobs in serial, 4gb memory should be enough"""
	outcnames = ['nllr_1', 'kld_1', 'cs_1', 'nllr_2', 'kld_2', 'cs_2',
		'nllr_3', 'kld_3', 'cs_3', 'nllr_ocr', 'kld_ocr', 'cs_ocr']
	RunTLM(outcnames).run(WEIGHTED)



if __name__ == '__main__':
	# run_serial()
	# run_parallel()
	RunTLM([]).output_rtmatrixes()
