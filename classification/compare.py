#!/usr/bin/env python

"""
Compares performances of different classifiers.

Siyuan Guo, Apr 2014
"""

from scipy.stats import ttest_rel
from numpy import mean
from prepare import Data
from model import BL, LR, DT, SVM, Classifier
from itertools import combinations

def fetch_data():
	"""Fetch data"""
	print 'Preparing data for analysis...'
	datamodel = Data()
	datamodel.add_date_features()
	# datamodel.add_ocr_features()
	datamodel.add_nllr_features()
	# datamodel.add_kld_features()
	datamodel.add_cs_features()
	data = datamodel.get_data()
	print 'Data is ready: [{0} rows x {1} columns]'.format(*data.shape)
	return data

READYDATA = fetch_data()

def run(clf):
	"""
	Run a classifier.
	@param clf, a classifier.
	@return precision, recall and f1 scores.
	"""
	if not issubclass(clf, Classifier):
		raise TypeError("Argument should be an instance of Classifier class.")
	model = clf(READYDATA)
	print "Running %s..." % str(model)
	model.repeat(100)
	return model.evaluate()

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
	compare_all()
