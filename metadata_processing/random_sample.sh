# use this one line bash.sh perl command to randomly sample lines from a flat file
# modify gt value to return that percentage of total lines, here 85%
# replace 'source_file.txt' and 'out_file.txt' with your source data and output file respectively

# Created by Trevor Edelblute, Mar 2014.

perl -ne 'print if (rand() < .85)' source_file.txt > out_file.txt

