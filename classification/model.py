#!/usr/bin/env python

"""
Siyuan Guo, Apr 2014

All classifiers are implemented in this module.
"""

from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.svm import LinearSVC


class Classifier(object):
	"""
	Base classifier class for different models.
	Intialization require prepared data.
	"""

	def __init__(self, data, train_indices, test_indices):
		self.xtrain, self.ytrain, self.xtest, self.ytest = self.split(data, train_indices, test_indices)

	@staticmethod
	def split(data, train_indices, test_indices):
		"""
		Split dataset into training & testing data, then features(X) & outcomes(y)
		@param train_indices, indices for training dataset generated by stratified
		                      K-fold cross-validation.
		@param test_indices, indices for testing dataset generated by stratified 
		                     K-fold cross-validation.
	 	"""
		train = data.iloc[train_indices,]
		test = data.iloc[test_indices,]
		features = list(set(list(test.columns.values)) - set(['range', '_id']))
		xtrain, ytrain = train[features], list(train['range'])
		xtest, ytest = test[features], list(test['range'])
		return xtrain, ytrain, xtest, ytest

	def fit_and_predict(self):
		"""Placeholder for 'fit_and_predict' method"""
		raise NotImplementedError("'fit_and_predict' method isn't implemented.")


class BL(Classifier):
	"""Baseline (Pick 'firstrange' as prediction)"""

	NAME = "Baseline"
	LABEL = "BL"

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

	NAME = "Logistic Regression"
	LABEL = "LR"

	def fit_and_predict(self):
		model = LogisticRegression()
		model.fit(self.xtrain, self.ytrain)
		ypred = model.predict(self.xtest)
		return self.ytest, ypred

class SVM(Classifier):
	"""Linear Support Vector Machine (one-vs-one)"""

	NAME = "Support Vector Machine"
	LABEL = "SVM"

	def fit_and_predict(self):
		model = LinearSVC()
		model.fit(self.xtrain, self.ytrain)
		ypred = model.predict(self.xtest)
		return self.ytest, ypred

class DT(Classifier):
	"""Decision Tree"""

	NAME = "Decision Tree"
	LABEL = "DT"

	def fit_and_predict(self):
		model = DecisionTreeClassifier()
		model.fit(self.xtrain, self.ytrain)
		ypred = model.predict(self.xtest)
		return self.ytest, ypred
