#!/usr/bin/env python

"""
Compares performances of different classifiers.

Siyuan Guo, Apr 2014
"""

from scipy.stats import ttest_rel
from numpy import mean
from prepare import Data
from model import BL, LR, DT, SVM
from sklearn.metrics import precision_score, recall_score, f1_score, confusion_matrix
from itertools import combinations
import cPickle, os



def fetch_data():
	"""Fetch data"""
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



READYDATA = fetch_data()
METRICS = ['nllr', 'kld', 'cs']
DATERANGES = [u"pre-1839", u"1840-1860", u"1861-1876", u"1877-1887", 
			  u"1888-1895", u"1896-1901", u"1902-1906", u"1907-1910", 
			  u"1911-1914", u"1915-1918", u"1919-1922", u"1923-present"]
DATECOLS = DATERANGES + [l+'-1st' for l in DATERANGES]
OCRCOLS = {m:[l+'-'+m+'_ocr' for l in DATERANGES] for m in METRICS}
UNICOLS = {m:[l+'-'+m+'_1' for l in DATERANGES] for m in METRICS}
BICOLS = {m:[l+'-'+m+'_2' for l in DATERANGES] for m in METRICS}
TRICOLS = {m:[l+'-'+m+'_3' for l in DATERANGES] for m in METRICS}



def job_wrapper(args):
	"""
	Wrapper for a parallel job.
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
	pool = Pool(2)
	results = pool.map(job_wrapper, [[clf, columns]] * num)
	return results # fix this

def print_cm(cm, labels):
	"""pretty print for confusion matrixes"""
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
			print "%{0}d".format(columnwidth) % cm[i, j],
		print

def evaluate(results, output_cm=False):
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

def run(clf, columns):
	"""
	Run a classifier.
	@param clf, a classifier, one of BL, LR, DT and SVM.
	@param columns, the columns to be used in data.
	@return precision, recall and f1 scores.
	"""
	print " Running %s..." % str(clf)
	return evaluate(repeat(clf, columns, 2))



def ttest(prf1, prf2):
	"""
	Run t-test to compare means

	@param prf1, a 3-tuple: precision, recall and fscore of a classifier.
	@param prf2, a 3-tuple: precision, recall and fscore of another classifier.
	"""
	p1,r1,f1 = prf1
	p2,r2,f2 = prf2
	# show means
	print
	print '    Precision means: ', mean(p1), mean(p2)
	print '    Recall    means: ', mean(r1), mean(r2)
	print '    F-scores  means: ', mean(f1), mean(f2)
	# compare model performance using paired t-test
	print '    Precision - Paired t-test: t={0}, p={1}'.format(*ttest_rel(p1, p2))
	print '    Recall    - Paired t-test: t={0}, p={1}'.format(*ttest_rel(r1, r2))
	print '    F-score   - Paired t-test: t={0}, p={1}'.format(*ttest_rel(f1, f2))

def compare_pairs(pairs):
	"""
	Compare pairs of performances.

	@param pairs, a list of 2-tuples (name, a list of p/r/f scores).
	"""
	for pair1, pair2 in combinations(pairs, 2):
		name1, prf1 = pair1
		name2, prf2 = pair2
		print '  Compare {0} and {1} models:'.format(name1, name2)
		ttest(prf1, prf2)
		print
	
def eval_date():
	"""
	Compare performances of 4 classifiers with only date features.
	"""
	columns = [u'range'] + DATECOLS
	pairs = zip(
		['BL', 'LR', 'DT', 'SVM'], 
		[run(clf, columns) for clf in BL, LR, DT, SVM]
		)
	compare_pairs(pairs)

def eval_text(addon=[]):
	"""
	Compare performances of 3 classifiers with only text features.
	"""
	for m in METRICS:
		print '\nUse %s metric...' % m
		for clf in [LR, DT, SVM]:
			print 'Use %s classifier...' % str(clf)
			incremental_features = [
				[u'range'] + OCRCOLS[m], 
				[u'range'] + OCRCOLS[m] + UNICOLS[m],
				[u'range'] + OCRCOLS[m] + UNICOLS[m] + BICOLS[m],
				[u'range'] + OCRCOLS[m] + UNICOLS[m] + BICOLS[m] + TRICOLS[m]
				]
			incremental_features = [x+addon for x in incremental_features]
			pairs = zip(
				['ocr', 'ocr+uni', 'ocr+uni+bi', 'ocr+uni+bi+tri'], 
				[run(clf, columns) for columns in incremental_features]
				)
			compare_pairs(pairs)

def eval_date_n_text():
	"""
	Compare performances of 3 classifiers with both date and text features.
	"""
	eval_text(DATECOLS)



if __name__ == '__main__':
	# READYDATA.to_csv('data.csv')
	# eval_date()
	# eval_text()
	eval_date_n_text()
