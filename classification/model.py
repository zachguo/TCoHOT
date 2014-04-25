#!/usr/bin/env python

"""
Siyuan Guo, Apr 2014

All classifiers are implemented in this module.
"""

from random import sample
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.svm import LinearSVC


class Classifier(object):
	"""
	Base classifier class for different models.
	Intialization require prepared data.
	"""

	def __init__(self, data):
		self.xtrain, self.ytrain, self.xtest, self.ytest = self.sample_and_split(data)

	@staticmethod
	def sample_and_split(data):
		"""
		Randomly sample 4/5 as training data, rest as testing data, then
		split each dataset into features(X) and outcomes(y)
	 	"""
		i_sampled = sample(data.index, len(data.index)*4/5)
		train = data.ix[i_sampled]
		test = data.drop(i_sampled)
		features = list(set(list(test.columns.values)) - set(['range', '_id']))
		xtrain, ytrain = train[features], list(train['range'])
		xtest, ytest = test[features], list(test['range'])
		return xtrain, ytrain, xtest, ytest

	def fit_and_predict(self):
		"""Placeholder for 'fit_and_predict' method"""
		raise NotImplementedError("'fit_and_predict' method isn't implemented.")


class BL(Classifier):
	"""Baseline (Pick 'firstrange' as prediction)"""

	def __repr__(self):
		return "Baseline"

	def fit_and_predict(self):
		xtest = self.xtest
		featurelabels = [x for x in xtest.columns if x.endswith('-1st')]
		if not featurelabels:
			raise ValueError('Required date features don\'t exist')
		xtest = xtest[featurelabels]
		true_colname = lambda x: xtest.columns[(x == True).tolist().index(True)][:-4]
		ypred = xtest.apply(true_colname, axis=1).tolist()
		return self.ytest, ypred

class LR(Classifier):
	"""Logistic Regression (one-vs-all)"""

	def __repr__(self):
		return "Logistic Regression"

	def fit_and_predict(self):
		model = LogisticRegression()
		model.fit(self.xtrain, self.ytrain)
		ypred = model.predict(self.xtest)
		return self.ytest, ypred

class SVM(Classifier):
	"""Linear Support Vector Machine (one-vs-one)"""

	def __repr__(self):
		return "Support Vector Machine"

	def fit_and_predict(self):
		model = LinearSVC()
		model.fit(self.xtrain, self.ytrain)
		ypred = model.predict(self.xtest)
		return self.ytest, ypred

class DT(Classifier):
	"""Decision Tree"""

	def __repr__(self):
		return "Decision Tree"

	def fit_and_predict(self):
		model = DecisionTreeClassifier()
		model.fit(self.xtrain, self.ytrain)
		ypred = model.predict(self.xtest)
		return self.ytest, ypred
