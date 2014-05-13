#!/usr/bin/env python

"""
Compares performances of different classifiers.

Siyuan Guo, Apr 2014
"""

from prepare import Data
from model import BL, LR, DT, SVM
from sklearn.metrics import precision_score, recall_score, f1_score, confusion_matrix
# from scipy.stats import ttest_rel
from pandas import DataFrame
import cPickle, os



####################
# Helper Functions #
####################

def fetch_data():
	"""
	Fetch data into a dataframe.
	"""
	print 'Preparing data for analysis...'
	if os.path.exists('data.cache'):
		print 'Loading data from cache...'
		with open('data.cache') as fin:
			data = cPickle.load(fin)
	else:
		datamodel = Data()
		datamodel.add_date_features()
		datamodel.add_nllr_features()
		datamodel.add_kld_features()
		datamodel.add_cs_features()
		data = datamodel.get_data()
		with open('data.cache', 'w') as fout:
			cPickle.dump(data, fout)
	print 'Data is ready: [{0} rows x {1} columns]'.format(*data.shape)
	return data

def print_cm(confmat, labels):
	"""
	Pretty print for confusion matrixes, used by `get_prf` function.
	"""
	columnwidth = max([len(x) for x in labels])
	# Print header
	print " " * columnwidth,
	for label in labels: 
		print "%{0}s".format(columnwidth) % label,
	print
	# Print rows
	for i, label1 in enumerate(labels):
		print "%{0}s".format(columnwidth) % label1,
		for j in range(len(labels)): 
			print "%{0}d".format(columnwidth) % confmat[i, j],
		print



#############
# Constants #
#############

READYDATA = fetch_data()
METRICS = ['nllr', 'kld', 'cs']
DATERANGES = [u"pre-1839", u"1840-1860", u"1861-1876", u"1877-1887", 
			  u"1888-1895", u"1896-1901", u"1902-1906", u"1907-1910", 
			  u"1911-1914", u"1915-1918", u"1919-1922", u"1923-present"]
DVCOL = [u'range']
DATECOLS = DATERANGES + [l+'-1st' for l in DATERANGES]
OCRCOLS = {m:[l+'-'+m+'_ocr' for l in DATERANGES] for m in METRICS}
UNICOLS = {m:[l+'-'+m+'_1' for l in DATERANGES] for m in METRICS}
BICOLS = {m:[l+'-'+m+'_2' for l in DATERANGES] for m in METRICS}
TRICOLS = {m:[l+'-'+m+'_3' for l in DATERANGES] for m in METRICS}



########################
# Evaluate Classifiers #
########################

def job_wrapper(args):
	"""
	Wrapper for a parallel job, used by `repeat` function.
	@param args, a 2-tuple of clf and columns.
	"""
	clf, columns = args
	return clf(READYDATA[columns]).fit_and_predict()

def repeat(clf, columns, num=10):
	"""
	Run the specified model n times in parallel, and return the results.
	@param clf, a classifier, one of BL, LR, DT and SVM.
	@param columns, the columns to be used in data.
	@param num, number of repeats
	@return results, a list of 2-tuples result [(ypred, ytest), ...]
	"""

	from multiprocessing import Pool
	pool = Pool()
	results = pool.map(job_wrapper, [[clf, columns]] * num)
	pool.close()
	return results

def get_prf(results, output_cm=False):
	"""
	Take the output of `repeatRun` as input to produce model evaluations.
	@param results, a list of 2-tuples result [(ypred, ytest), ...]
	@param output_cm, whether or not print confusion_matrix
	@return a list of precisions, recalls, f1s
	"""
	# Generate precision, recall and f1 scores
	metrics = [precision_score, recall_score, f1_score]
	f = lambda m: [m(*pair) for pair in results]
	precisions, recalls, f1s = [f(m) for m in metrics]
	# Print confusion matrix
	if output_cm:
		cms = [confusion_matrix(*(list(pair)+[DATERANGES])) for pair in results]
		# Get an average confusion matrix, note that 
		# I use integer division here for readability
		cm_avg = reduce(lambda x, y: x+y, cms) / len(cms)
		print_cm(cm_avg, DATERANGES)
	return precisions, recalls, f1s



class Evaluation(object):
	"""
	Generate evaluation results to be used in the paper.
	"""

	def __init__(self):
		self.results = DataFrame()

	def output(self, filename):
		self.results.to_csv(filename)
		self.results = DataFrame()

	def eval_date(self, output=True):
		"""
		Compare 4 classifiers x only date features.
		@param output, whether or not to output results as a csv file, default True.
		"""
		columns = DVCOL + DATECOLS
		for clf in (BL, LR, DT, SVM):
			print '  Use %s classifier...' % clf.NAME
			prfs_named = zip(('p', 'r', 'f'), get_prf(repeat(clf, columns)))
			for sname, scores in prfs_named:
				col_label = '_'.join([sname, clf.LABEL, 'd'])
				self.results[col_label] = scores
		if output:
			self.output('results_date.csv')

	def eval_text(self, features, fnames):
		"""
		Compare 3 classifiers x 3 metrics x only text features.
		@param features, a dict of features, metric is the key, values are a list of 
		                 features.
		@param fnames, a list of feature names.
		"""
		for m in METRICS:
			print '  Use %s metric...' % m
			for clf in (LR, DT, SVM):
				print '    Use %s classifier...' % clf.NAME
				features_named = zip(fnames, features[m])
				for fname, columns in features_named:
					print '      Use %s feature set...' % fname
					prfs_named = zip(('p', 'r', 'f'), get_prf(repeat(clf, columns)))
					for sname, scores in prfs_named:
						col_label = '_'.join([sname, clf.LABEL, m, fname])
						self.results[col_label] = scores

	def eval_text_incremental(self, output=True):
		"""
		Compare 3 classifiers x 3 metrics x only incremental text features.
		@param output, whether or not to output results as a csv file, default True.
		"""
		features = {}
		for m in METRICS:
			features[m] = (
				DVCOL + OCRCOLS[m], 
				DVCOL + OCRCOLS[m] + UNICOLS[m],
				DVCOL + OCRCOLS[m] + UNICOLS[m] + BICOLS[m],
				DVCOL + OCRCOLS[m] + UNICOLS[m] + BICOLS[m] + TRICOLS[m]
				)
		fnames = ('o', 'ou', 'oub', 'oubt')
		self.eval_text(features, fnames)
		if output:
			self.output('results_text_inc.csv')

	def eval_text_individual(self, output=True):
		"""
		Compare 3 classifiers x 3 metrics x only individual text features.
		@param output, whether or not to output results as a csv file, default True.
		"""
		features = {}
		for m in METRICS:
			features[m] = (
				DVCOL + OCRCOLS[m], 
				DVCOL + UNICOLS[m],
				DVCOL + BICOLS[m],
				DVCOL + TRICOLS[m]
				)
		fnames = ('o', 'u', 'b', 't')
		self.eval_text(features, fnames)
		if output:
			self.output('results_text_ind.csv')

	def eval_date_n_text(self, output=True):
		"""
		Compare 3 classifiers x 3 metrics x both date and text features.
		@param output, whether or not to output results as a csv file, default True.
		"""
		features = {}
		for m in METRICS:
			ftrs = (
				DVCOL + OCRCOLS[m], 
				DVCOL + OCRCOLS[m] + UNICOLS[m],
				DVCOL + OCRCOLS[m] + UNICOLS[m] + BICOLS[m],
				DVCOL + OCRCOLS[m] + UNICOLS[m] + BICOLS[m] + TRICOLS[m]
				)
			features[m] = [x+DATECOLS for x in ftrs]
		fnames = ('do', 'dou', 'doub', 'doubt')
		self.eval_text(features, fnames)
		if output:
			self.output('results_datetext.csv')

	def eval_no_char(self, output=True):
		"""
		Compare 3 classifiers x 3 metrics x only text (without character level)
		features.
		"""
		features = {}
		for m in METRICS:
			features[m] = (
				DVCOL + UNICOLS[m],
				DVCOL + UNICOLS[m] + BICOLS[m],
				DVCOL + UNICOLS[m] + BICOLS[m] + TRICOLS[m]
				)
		fnames = ('u', 'ub', 'ubt')
		self.eval_text(features, fnames)
		if output:
			self.output('results_noocr.csv')

	def run(self, output=True):
		"""
		Run all evaluations.
		@param output, whether or not to output results as a csv file, default True.
		"""
		print '\nCompare 4 classifiers x only date features.'
		self.eval_date(output)
		print '\nCompare 3 classifiers x 3 metrics x only text features.'
		self.eval_text_incremental(output)
		print '\nCompare 3 classifiers x 3 metrics x only individual text features.'
		self.eval_text_individual()
		print '\nCompare 3 classifiers x 3 metrics x all text but character features.'
		self.eval_no_char()
		print '\nCompare 3 classifiers x 3 metrics x both date and text features.'
		self.eval_date_n_text(output)



if __name__ == '__main__':
	# READYDATA.to_csv('data.csv')
	Evaluation().run()
