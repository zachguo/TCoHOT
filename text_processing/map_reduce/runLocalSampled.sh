#!/bin/bash

# randomly sample 100 text files from original file foler
# then generate TFIDF based on this sample
# repeat such process 10 times
# each process may take several minutes

# Put it in a folder where there are `joinedtable.csv` and an folder 'aa' contains all text corpora
# mongod should be running

# Created by Siyuan Guo, Mar 2014.

for i in {1..10}
do
	mkdir $i
	mkdir $i/corpora
	perl -ne 'print if (rand() < .01)' joinedtable.csv | head -100 | cut -f 2 | sed 's/$/.txt/' | sed 's/^/aa\//' | xargs -J % cp % $i/corpora

	export HTRC_TEXT_CONCAT_PATH=/Users/syg/Documents/corpora/hathitrust/text/concat_data/$i/corpora
	export MAPREDUCE_CODES_PATH=/Users/syg/Dropbox/Projects/Z604-Project/text_processing/map_reduce/getTF
	export TFIDF_PATH=/Users/syg/Dropbox/Projects/Z604-Project/text_processing/

	chmod +x $MAPREDUCE_CODES_PATH/mapper.py
	chmod +x $MAPREDUCE_CODES_PATH/reducer.py

	cd $i
	for j in $( ls $HTRC_TEXT_CONCAT_PATH); do
		export map_input_file=fakepath/$j
		cat $HTRC_TEXT_CONCAT_PATH/$j | $MAPREDUCE_CODES_PATH/mapper.py | sort | $MAPREDUCE_CODES_PATH/reducer.py >> tf_raw_$i
	done
	python $TFIDF_PATH/getTFIDFMatrix.py tf_raw_$i
	cd ..

done