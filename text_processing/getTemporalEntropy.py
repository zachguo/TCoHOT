#! /usr/bin/env python

# Created by Siyuan Guo, Mar 28
# This script generate three term-timeslice matrixes
# Collections 'date','tf_1','tf_2','tf_3' must exist in mongoDB before execution

# A sample bigram term-timeslice matrix output:
#                      1840-1860  1861-1876  1877-1887 ... 1923-present  pre-1839  entropy  totalfreq
# sessional papers             0          1          0 ...            0         0       -0       8599
# speaker put                  0          0          0 ...            0         1       -0       3704
# ...
# [167615 rows x 14 columns]


from pymongo import MongoClient
from collections import defaultdict
import pandas, math

DATERANGES = ["pre-1839","1840-1860","1861-1876","1877-1887","1888-1895","1896-1901","1902-1906","1907-1910","1911-1914","1915-1918","1919-1922","1923-present"]

def main():
	client = MongoClient('localhost', 27017)
	db = client.HTRC
	missing_collections = set(['date','tf_1','tf_2','tf_3'])-set(db.collection_names())
	if missing_collections:
		print "Collections %s doesn't exist in 'HTRC' database. Task aborted." % '&'.join(missing_collections)
		return

	# read all doc_ids for each date range from mongoDB
	daterange_docid_dict = {} # {'pre-1839':['loc.ark+=13960=t9h42611g', ...], ...}
	for dr in DATERANGES:
		daterange_docid_dict[dr] = [ e['_id'] for e in db.date.find({"range":dr},{"raw":0, "range":0}) ]

	# read term frquencies from mongoDB for each doc_id, then aggregate tf by timeslice
	collections = zip(['uni','bi','tri'],[db.tf_1,db.tf_2,db.tf_3])
	for cname,c in collections:
		daterange_tf_dict = {} # {'pre-1839':{u'': 273885.0, u'1,808': 14.0, u'woode': 6.0, u'tripolitan': 30.0, ...}, ...}
		for dr in DATERANGES:
			daterange_tf_dict[dr] = defaultdict(int)
			doc_ids = daterange_docid_dict[dr]
			for doc_id in doc_ids:
				try:
					# read raw term frequencies for each doc_id from mongoDB
					tfdict = c.find_one({"_id":doc_id})['freq']
					# merge & sum term frequencies for each date range
					for term in tfdict:
						daterange_tf_dict[dr][term] += tfdict[term]
				except:
					print "No term frequency for doc %s." % doc_id

		# A lot of lambdas & idiomatic pandas functions will be used, they are superfast!
		# Convert 2D dictionary into pandas dataframe (named matrix)
		date_term_matrix = pandas.DataFrame(daterange_tf_dict).fillna(0)

		# Reorder columns
		date_term_matrix = date_term_matrix[DATERANGES]

		# Filter terms(rows) with too small frequency (less than 50)
		date_term_matrix = date_term_matrix[date_term_matrix.apply(lambda x:sum(x)>=50, axis=1)]
		
		# Normalize each row from freq to prob
		rowsums = date_term_matrix.sum(axis=1)
		date_term_matrix = date_term_matrix.div(rowsums, axis=0)
		# Have a look at a single term(row):
		# print date_term_matrix.loc['better',:]

		# Calculate entropy for each row(distribution across timeslices for each term), then append calculated result as a new column
		date_term_matrix["entropy"] = date_term_matrix.applymap(lambda x:-x*math.log10(x+0.000001)).sum(axis=1)
		date_term_matrix['totalfreq'] = rowsums
		date_term_matrix.sort(['entropy', 'totalfreq'], ascending=[1, 0]).to_csv('date_%sgram_matrix.tsv'%cname, sep='\t', encoding='utf-8')

if __name__ == '__main__':
	main()