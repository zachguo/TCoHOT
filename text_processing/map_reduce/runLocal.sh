#! /bin/bash

# Created by Siyuan Guo, Mar 2014

# Change paths below before running the script
export HTRC_TEXT_CONCAT_PATH=/Users/syg/Documents/corpora/hathitrust/concat_data/aa
# export MAPREDUCE_CODES_PATH=/Users/syg/Dropbox/Projects/Z604-Project/text_processing/mapreduce/getDateInText
export MAPREDUCE_CODES_PATH=/Users/syg/Dropbox/Projects/Z604-Project/text_processing/mapreduce/getTF

chmod +x $MAPREDUCE_CODES_PATH/mapper.py
chmod +x $MAPREDUCE_CODES_PATH/reducer.py

for i in $( ls $HTRC_TEXT_CONCAT_PATH); do
	export map_input_file=fakepath/$i
	cat $HTRC_TEXT_CONCAT_PATH/$i | $MAPREDUCE_CODES_PATH/mapper.py | sort | $MAPREDUCE_CODES_PATH/reducer.py
done