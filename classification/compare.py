#!/usr/bin/env python

"""
Compares performances of different classifiers.

Siyuan Guo, Apr 2014
"""

from scipy.stats import ttest_rel
from numpy import mean
from prepare import Data
from model import BL, LR, DT, SVM, Classifier
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


def job_wrapper(clf):
	model = clf(READYDATA)
	return model.fit_and_predict()

def repeat(clf, num=10):
	"""
	Run the specified model n times, and return the results.
	@param n, number of repeats
	@param clf, the classifier to be used.
	@return results, a list of 2-tuples result [(ypred, ytest), ...]
	"""

	from multiprocessing import Pool
	pool = Pool(2)
	results = pool.map(job_wrapper, [clf] * num)
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
	labels = ["pre-1839", "1840-1860", "1861-1876", "1877-1887", "1888-1895", 
			"1896-1901", "1902-1906", "1907-1910", "1911-1914", "1915-1918", 
			"1919-1922", "1923-present"]
	# Generate precision, recall and f1 scores
	metrics = [precision_score, recall_score, f1_score]
	f = lambda m: [m(*pair) for pair in results]
	precisions, recalls, f1s = [f(m) for m in metrics]
	# Print confusion matrix
	if output_cm:
		cms = [confusion_matrix(*(list(pair)+[labels])) for pair in results]
		# Get an average confusion matrix, note that 
		# I use integer division here for readability
		cm_avg = reduce(lambda x, y: x+y, cms) / len(cms)
		print_cm(cm_avg, labels)
	return precisions, recalls, f1s



def run(clf):
	"""
	Run a classifier.
	@param clf, a classifier.
	@return precision, recall and f1 scores.
	"""
	if not issubclass(clf, Classifier):
		raise TypeError("Argument should be an instance of Classifier class.")
	print "Running %s..." % str(clf)
	return evaluate(repeat(clf))



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
	print 'Precision means: ', mean(p1), mean(p2)
	print 'Recall    means: ', mean(r1), mean(r2)
	print 'F-scores  means: ', mean(f1), mean(f2)
	print
	# compare model performance using paired t-test
	print 'Precision - Paired t-test: t={0}, p={1}'.format(*ttest_rel(p1, p2))
	print 'Recall    - Paired t-test: t={0}, p={1}'.format(*ttest_rel(r1, r2))
	print 'F-score   - Paired t-test: t={0}, p={1}'.format(*ttest_rel(f1, f2))



def compare(clf1, clf2):
	"""
	Compare performances of a pair of classifiers.
	@param clf1, first classifier.
	@param clf1, second classifier.
	"""
	if not (issubclass(clf1, Classifier) and issubclass(clf2, Classifier)):
		raise TypeError("Arguments should be instances of Classifier class.")
	print '\nCompare {0} & {1}:\n'.format(str(clf1), str(clf2))
	p1,r1,f1 = run(clf1)
	p2,r2,f2 = run(clf2)
	ttest((p1,r1,f1), (p2,r2,f2))
	
def compare_all():
	"""
	Compare performances of all classifiers.
	"""
	prfs = zip(['BL', 'LR', 'DT', 'SVM'], [run(clf) for clf in BL, LR, DT, SVM])
	for prf1, prf2 in combinations(prfs, 2):
		name1, (p1,r1,f1) = prf1
		name2, (p2,r2,f2) = prf2
		print 'Compare {0} and {1} models:'.format(name1, name2)
		ttest((p1,r1,f1), (p2,r2,f2))
		print

if __name__ == '__main__':
	# compare_all()
	# READYDATA.to_csv('data.csv')
	print run(LR)
