#! /bin/bash

# Created by Siyuan Guo, Apr 2014

# Run map reduce codes locally with GNU Parallel, please install parallel first.

# Change paths below before running the script
export HTRC_TEXT_CONCAT_PATH=/Users/syg/Documents/corpora/hathitrust/corpora/concat_data/aa
# export MAPREDUCE_CODES_PATH=/Users/syg/Dropbox/Projects/Z604-Project/text_processing/map_reduce/getDateInText
export MAPREDUCE_CODES_PATH=/Users/syg/Dropbox/Projects/Z604-Project/text_processing/map_reduce/getTF

chmod +x $MAPREDUCE_CODES_PATH/mapper.py
chmod +x $MAPREDUCE_CODES_PATH/reducer.py

parallel 'export map_input_file=fakepath/{}; cat {} | $MAPREDUCE_CODES_PATH/mapper.py | sort | $MAPREDUCE_CODES_PATH/reducer.py' ::: $HTRC_TEXT_CONCAT_PATH/*.txt