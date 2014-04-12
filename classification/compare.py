#!/usr/bin/env python

"""
Compares performances of different classifiers.

Siyuan Guo, Apr 2014
"""

from scipy.stats import ttest_rel
from numpy import mean
from prepare import Data
from model import BL, LR, DT, SVM, Classifier

def fetch_data():
	"""Fetch data"""
	datamodel = Data()
	datamodel.add_date_features()
	# datamodel.add_nllr_features()
	datamodel.add_kld_features()
	return datamodel.get_data()

def run(clf):
	"""
	Run a classifier.
	@param clf, a classifier.
	@return precision, recall and f1 scores.
	"""
	if not issubclass(clf, Classifier):
		raise TypeError("Argument should be an instance of Classifier class.")
	model = clf(fetch_data())
	print "Running %s..." % str(model)
	model.repeat(100)
	return model.evaluate()

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
	# show mean and std
	print
	print 'Precision means: ', mean(p1), mean(p2)
	print 'Recall    means: ', mean(r1), mean(r2)
	print 'F-scores  means: ', mean(f1), mean(f2)
	print
	# compare model performance using paired t-test
	print 'Precision - Paired t-test: t={0}, p={1}'.format(*ttest_rel(p1, p2))
	print 'Recall    - Paired t-test: t={0}, p={1}'.format(*ttest_rel(r1, r2))
	print 'F-score   - Paired t-test: t={0}, p={1}'.format(*ttest_rel(f1, f2))

def compare_all():
	"""
	Compare performances of all classifiers.
	"""
	[run(clf) for clf in BL, LR, DT, SVM]

if __name__ == '__main__':
	compare(BL, LR)
