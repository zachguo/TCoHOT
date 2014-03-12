#! /usr/bin/env python

# Created by Siyuan Guo, Mar 2014.

import pandas, glob, scipy.stats
from sklearn.metrics import precision_score, recall_score, f1_score

p_naive,p_lr,r_naive,r_lr,f_naive,f_lr = [],[],[],[],[],[]
for fn in glob.glob('/Users/syg/Documents/corpora/hathitrust/processed_data/outcome/*.csv'):
	with open(fn) as fin:
		df = pandas.read_csv(fn,header=0)
		p_naive.append(precision_score(list(df['gs']),list(df['pred_naive'])))
		p_lr.append(precision_score(list(df['gs']),list(df['pred'])))
		r_naive.append(recall_score(list(df['gs']),list(df['pred_naive'])))
		r_lr.append(recall_score(list(df['gs']),list(df['pred'])))
		f_naive.append(f1_score(list(df['gs']),list(df['pred_naive'])))
		f_lr.append(f1_score(list(df['gs']),list(df['pred'])))
df_eval = pandas.DataFrame(zip(p_naive,p_lr,r_naive,r_lr,f_naive,f_lr),columns=["p_naive","p_lr","r_naive","r_lr","f_naive","f_lr"])
print 'mean:\n',df_eval.mean()
print 'std:\n',df_eval.std()
print 'Precision Paired t-test:\n',scipy.stats.ttest_rel(df_eval['p_naive'],df_eval['p_lr'])
print 'Recall Paired t-test:\n',scipy.stats.ttest_rel(df_eval['r_naive'],df_eval['r_lr'])
print 'F-score Paired t-test:\n',scipy.stats.ttest_rel(df_eval['f_naive'],df_eval['f_lr'])