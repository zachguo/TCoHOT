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
	pool.terminate()
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

	def eval_date(self):
		"""
		Compare 4 classifiers x only date features.
		"""
		columns = DVCOL + DATECOLS
		for clf in (BL, LR, DT, SVM):
			print '  Use %s classifier...' % clf.NAME
			prfs_named = zip(('p', 'r', 'f'), get_prf(repeat(clf, columns)))
			for sname, scores in prfs_named:
				col_label = '_'.join([sname, clf.LABEL, 'd'])
				self.results[col_label] = scores

	def eval_text(self, addon=('', [])):
		"""
		Compare 3 classifiers x 3 metrics x only text features.
		@param addon, default as ('', []), or receive a (('d', DATECOLS)), for
		              refactoring `eval_date_n_text` method.
		"""
		for m in METRICS:
			print '  Use %s metric...' % m
			for clf in (LR, DT, SVM):
				print '    Use %s classifier...' % clf.NAME
				features = (
					DVCOL + OCRCOLS[m], 
					DVCOL + OCRCOLS[m] + UNICOLS[m],
					DVCOL + OCRCOLS[m] + UNICOLS[m] + BICOLS[m],
					DVCOL + OCRCOLS[m] + UNICOLS[m] + BICOLS[m] + TRICOLS[m]
					)
				features = [x+addon[1] for x in features]
				fnames = [x+addon[0] for x in ('o', 'ou', 'oub', 'oubt')]
				features_named = zip(fnames, features)
				for fname, columns in features_named:
					print '      Use %s feature set...' % fname
					prfs_named = zip(('p', 'r', 'f'), get_prf(repeat(clf, columns)))
					for sname, scores in prfs_named:
						col_label = '_'.join([sname, clf.LABEL, m, fname])
						self.results[col_label] = scores

	def eval_date_n_text(self):
		"""
		Compare 3 classifiers x 3 metrics x both date and text features.
		"""
		self.eval_text(('d', DATECOLS))

	def run(self, output=True):
		"""
		Run all evaluations.
		@param output, whether or not to output results as a csv file, default True.
		"""
		print '\nCompare 4 classifiers x only date features.'
		self.eval_date()
		if output:
			self.results.to_csv('results_date.csv')
			self.results = DataFrame()
		print '\nCompare 3 classifiers x 3 metrics x only text features.'
		self.eval_text()
		if output:
			self.results.to_csv('results_text.csv')
			self.results = DataFrame()
		print '\nCompare 3 classifiers x 3 metrics x both date and text features.'
		self.eval_date_n_text()
		if output:
			self.results.to_csv('results_datetext.csv')
			self.results = DataFrame()



if __name__ == '__main__':
	# READYDATA.to_csv('data.csv')
	Evaluation().run()
