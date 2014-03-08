# Get frequencies of dates from MongoDB

# Created by Siyuan Guo, Mar 2014.

mongoexport --db HTRC --collection dv --csv --fields "date" | sed '1d' | cut -c 2-5 | grep '\d\d\d\d' | sort | uniq -c > alldatefreq.txt