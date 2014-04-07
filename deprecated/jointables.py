#! /usr/bin/env python

# Created by Siyuan Guo, Mar 2014.

import pandas as pd

DV_FILEPATH = '/Users/syg/Documents/corpora/hathitrust/processed_data/dv.txt'
DIT_FILEPATH = '/Users/syg/Documents/corpora/hathitrust/processed_data/date_in_text.csv'
DIT1ST_FILEPATH = '/Users/syg/Documents/corpora/hathitrust/processed_data/DateInText1st_aa.txt'
OUT_FILEPATH = 'joinedtable.csv'

dv = pd.read_csv(DV_FILEPATH, header=0, sep='\t')
dit = pd.read_csv(DIT_FILEPATH, header=0, sep='\t')
dit1st = pd.read_csv(DIT1ST_FILEPATH, header=0, sep='\t')

df = pd.merge(dv, dit, on='doc_id', how='inner')
df = pd.merge(df, dit1st, on='doc_id', how='inner')

df.to_csv(OUT_FILEPATH, sep='\t')
