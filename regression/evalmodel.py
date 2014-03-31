#! /usr/bin/env python

# Created by Siyuan Guo, Mar 2014.

import pandas, glob, scipy.stats
from sklearn.metrics import precision_score, recall_score, f1_score, confusion_matrix

# initialize lists of precision('p_'), recall('r_'), f1('f_') and confusion matrixes('cm_')
# for both naive and logistic regression model
p_naive,p_lr,r_naive,r_lr,f_naive,f_lr,cm_naive,cm_lr = [],[],[],[],[],[],[],[]
labels = ["pre-1839","1840-1860","1861-1876","1877-1887","1888-1895","1896-1901","1902-1906","1907-1910","1911-1914","1915-1918","1919-1922","1923-present"]

# read model outcomes and update precision, recall and f1 lists
for fn in glob.glob('/Users/syg/Documents/corpora/hathitrust/processed_data/midterm_models_outcomes/*.csv'):
	with open(fn) as fin:
		df = pandas.read_csv(fn,header=0)
		# 'gs' is golden standard, 'pred' is prediction by logistic regression, 'pred_naive' is prediction by naive model
		# get precision scores
		p_naive.append(precision_score(list(df['gs']),list(df['pred_naive'])))
		p_lr.append(precision_score(list(df['gs']),list(df['pred'])))
		# get recall scores
		r_naive.append(recall_score(list(df['gs']),list(df['pred_naive'])))
		r_lr.append(recall_score(list(df['gs']),list(df['pred'])))
		# get f1 scores
		f_naive.append(f1_score(list(df['gs']),list(df['pred_naive'])))
		f_lr.append(f1_score(list(df['gs']),list(df['pred'])))
		# get confusion matrixes
		cm_naive.append(confusion_matrix(list(df['gs']),list(df['pred_naive']), labels))
		cm_lr.append(confusion_matrix(list(df['gs']),list(df['pred']), labels))


# get an average confusion matrix, note that I use integer division here for readability
cm_naive = reduce(lambda x,y:x+y, cm_naive) / len(cm_naive)
cm_lr = reduce(lambda x,y:x+y, cm_lr) / len(cm_lr)

# pretty print for confusion matrixes
def print_cm(cm, labels):
	columnwidth = len(max(labels, key=lambda x:len(x)))
	# print header
	print " " * columnwidth,
	for label in labels: print "%{0}s".format(columnwidth) % label,
	print
	# print rows
	for i, label1 in enumerate(labels):
		print "%{0}s".format(columnwidth) % label1,
		for j, label2 in enumerate(labels): print "%{0}d".format(columnwidth) % cm[i, j],
		print

print '\nConfusion matrix for naive model:\n'
print_cm(cm_naive, labels)
# print pandas.DataFrame(cm_naive, index=labels, columns=labels) # can optionally convert confusion matrix to dataframe
print '\nConfusion matrix for logistic regression model:\n'
print_cm(cm_lr, labels)


# merge precision, recall and f1 lists into a dataframe
df_eval = pandas.DataFrame(zip(p_naive,p_lr,r_naive,r_lr,f_naive,f_lr),columns=["p_naive","p_lr","r_naive","r_lr","f_naive","f_lr"])
# descriptive statistics
print '\nMean:\n',df_eval.mean()
print '\nStd:\n',df_eval.std()
# compare model performance using paired t-test
print '\nPrecision - Paired t-test:\n',scipy.stats.ttest_rel(df_eval['p_naive'],df_eval['p_lr'])
print '\nRecall - Paired t-test:\n',scipy.stats.ttest_rel(df_eval['r_naive'],df_eval['r_lr'])
print '\nF-score - Paired t-test:\n',scipy.stats.ttest_rel(df_eval['f_naive'],df_eval['f_lr'])