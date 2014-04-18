#! /bin/bash

# Created by Siyuan Guo, Apr 2014

# Run map reduce codes locally with GNU Parallel, please install parallel first.

# This script receives 2 arguments, one is corpora path, the other one is MapReduce codes path.
# To run:
#	./runLocalParallel.sh path/to/aa path/to/getTF > tf_aa.txt
# Or:
#	./runLocalParallel.sh path/to/aa path/to/getDateInText > date_aa.txt

# Change paths below before running the script
export HTRC_TEXT_CONCAT_PATH=$1
export MAPREDUCE_CODES_PATH=$2

chmod +x $MAPREDUCE_CODES_PATH/mapper.py
chmod +x $MAPREDUCE_CODES_PATH/reducer.py

# To work in Ubuntu
# parallel --gnu 'export map_input_file=fakepath/{}; cat {} | $MAPREDUCE_CODES_PATH/mapper.py | sort | $MAPREDUCE_CODES_PATH/reducer.py' ::: $HTRC_TEXT_CONCAT_PATH/*.txt

# To work in OSX (without argument list too long error)
ls $HTRC_TEXT_CONCAT_PATH | grep \.txt | parallel 'export map_input_file=fakepath/{}; cat $HTRC_TEXT_CONCAT_PATH/{} | $MAPREDUCE_CODES_PATH/mapper.py | sort | $MAPREDUCE_CODES_PATH/reducer.py'
