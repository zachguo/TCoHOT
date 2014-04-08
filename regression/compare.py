#!/usr/bin/env python

"""
Siyuan Guo, Apr 2014

This module compares performances of different classifiers.
"""

from scipy.stats import ttest_rel
from numpy import mean, std
from prepare import preparedata
from model import BL, LR, DT, SVM
from types import TypeType

def run(clf):
	"""
	Run a classifier.
	@param clf, a classifier.
	@return precision, recall and f1 scores.
	"""
	if not isinstance(clf, TypeType):
		raise TypeError("Argument should be an instance of Classifier class.")
	model = clf(preparedata())
	model.repeat(50)
	return model.evaluate()

def compare(clf1, clf2):
	"""
	Print and compare performances of two classifiers.
	@param clf1, first classifier.
	@param clf1, second classifier.
	"""
	if not (isinstance(clf1, TypeType) and isinstance(clf2, TypeType)):
		raise TypeError("Arguments should be instances of Classifier class.")
	p1,r1,f1 = run(clf1)
	p2,r2,f2 = run(clf2)
	# show mean and std
	print 'Means: ', mean(f1), mean(f2)
	print 'Stds: ', std(f1), std(f2)
	# compare model performance using paired t-test
	print 'Precision - Paired t-test: t={0}, p={1}'.format(*ttest_rel(p1, p2))
	print 'Recall - Paired t-test: t={0}, p={1}'.format(*ttest_rel(r1, r2))
	print 'F-score - Paired t-test: t={0}, p={1}'.format(*ttest_rel(f1, f2))

if __name__ == '__main__':
	print 'Compare baseline & logistic regression:'
	compare(BL, LR)
	print 'Compare baseline & decision tree:'
	compare(BL, DT)
	print 'Compare baseline & support vector machine:'
	compare(BL, SVM)
