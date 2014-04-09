#!/usr/bin/env python

"""
Siyuan Guo, Apr 2014

All classifiers are implemented in this module.
"""

from random import sample
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.svm import SVC # We can also try other types of SVMs
from sklearn.metrics import precision_score, recall_score, f1_score, confusion_matrix


LABELS = ["pre-1839", "1840-1860", "1861-1876", "1877-1887", "1888-1895", 
		  "1896-1901", "1902-1906", "1907-1910", "1911-1914", "1915-1918", 
		  "1919-1922", "1923-present"]

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


class Classifier(object):
	"""
	Base classifier class for different models.
	Intialization require prepared data.
	"""

	def __init__(self, data):
		self.data = data
		self.results = []

	def get_data(self):
		"""Non-intrusive interface for data"""
		return self.data

	def sample_and_split(self):
		"""
		Randomly sample 4/5 as training data, rest as testing data, then
		split each dataset into features(X) and outcomes(y)
	 	"""
	 	data = self.get_data()
		i_sampled = sample(data.index, len(data.index)*4/5)
		train = data.ix[i_sampled]
		test = data.drop(i_sampled)
		features = list(set(list(test.columns.values)) - set(['range', '_id']))
		xtrain, ytrain = train[features], list(train['range'])
		xtest, ytest = test[features], list(test['range'])
		return xtrain, ytrain, xtest, ytest

	def fit_and_predict(self, xtrain, ytrain, xtest, ytest):
		"""Placeholder for 'fit_and_predict' method"""
		raise NotImplementedError("'fit_and_predict' method isn't implemented.")

	def repeat(self, num=10):
		"""
		Run the specified model n times, and return the results.
		@param n, number of repeats
		@param model, the model to be used.
		@return results, a list of 2-tuples result [(ypred, ytest), ...]
		"""
		self.results = [tuple(self.fit_and_predict(*self.sample_and_split())) for i in range(num)]

	def evaluate(self, output_cm=False):
		"""
		Take the output of `repeatRun` as input to produce model evaluations.
		@param output_cm, whether or not print confusion_matrix
		@return a list of precisions, recalls, f1s
		"""
		# Generate precision, recall and f1 scores
		metrics = [precision_score, recall_score, f1_score]
		f = lambda m: [m(*pair) for pair in self.results]
		precisions, recalls, f1s = [f(m) for m in metrics]
		# Print confusion matrix
		if output_cm:
			cms = [confusion_matrix(*(list(pair)+[LABELS])) for pair in self.results]
			# Get an average confusion matrix, note that 
			# I use integer division here for readability
			cm_avg = reduce(lambda x, y: x+y, cms) / len(cms)
			print_cm(cm_avg, LABELS)
		return precisions, recalls, f1s

class BL(Classifier):
	"""Baseline (Pick 'firstrange' as prediction)"""

	def __repr__(self):
		return "Baseline"

	def fit_and_predict(self, xtrain, ytrain, xtest, ytest):
		xtest = xtest[[x for x in xtest.columns if x.endswith('-1st')]]
		true_colname = lambda x: xtest.columns[(x == True).tolist().index(True)][:-4]
		ypred = (xtest.apply(true_colname, axis=1)).tolist()
		return ytest, ypred

class LR(Classifier):
	"""Logistic Regression (one-vs-all)"""

	def __repr__(self):
		return "Logistic Regression"

	def fit_and_predict(self, xtrain, ytrain, xtest, ytest):
		model = LogisticRegression()
		model.fit(xtrain, ytrain)
		ypred = model.predict(xtest)
		return ytest, ypred

class SVM(Classifier):
	"""Support Vector Machine (one-vs-one)"""

	def __repr__(self):
		return "Support Vector Machine"

	def fit_and_predict(self, xtrain, ytrain, xtest, ytest):
		model = SVC()
		model.fit(xtrain, ytrain)
		ypred = model.predict(xtest)
		return ytest, ypred

class DT(Classifier):
	"""Decision Tree"""

	def __repr__(self):
		return "Decision Tree"

	def fit_and_predict(self, xtrain, ytrain, xtest, ytest):
		model = DecisionTreeClassifier()
		model.fit(xtrain, ytrain)
		ypred = model.predict(xtest)
		return ytest, ypred
