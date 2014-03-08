# one line bash shell command to count year frequency of source file (date_range.txt) and write to output file datefreq.txt
# Created by Trevor Edelblute, Mar 2014.

awk -F '\t' '{print $4}' date_range.txt | sort | uniq -c | sort -nr >> datefreq.txt
