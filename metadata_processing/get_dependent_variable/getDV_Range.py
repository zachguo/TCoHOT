#! /usr/bin/env python

#Generate dependant variable doc date range
#classify doc date by date range 
# An example folder name is: /path/to/doc_ID_Date/
# To run: python getDV_Range.py /path/to/doc_ID_Date/

# Created by Bin Dai, Mar 2014.

import json,os,re,sys

def main(filename):
    with open(filename) as file:
        with open('doc_in_range.txt', 'w') as fout:
            for line in file:
                if line:
                    if len(line.split(',')) == 2:
                        _id, date = line.split(',')
                    elif len(line.split(',')) == 4:
                        id_1,id_2,id_3,date = line.split(',')
                        _id = id_4 = id_1+','+id_2+','+id_3
                    y = date[1:5]
                    print y
                    if y.isdigit():
                        fout.write('{0}\t{1}\t'.format(_id.strip('"'),y))
                        year = int(y)
                        if year <= 1839:
                            fout.write("pre-1839\n")
                        elif year>=1840 and year <= 1860:
                            fout.write("1840-1860\n")
                        elif year>=1861 and year <= 1876:
                            fout.write("1861-1876\n")
                        elif year>=1877 and year <= 1887:
                            fout.write("1877-1887\n")
                        elif year>=1888 and year <= 1895:
                            fout.write("1888-1895\n")
                        elif year>=1896 and year <= 1901:
                            fout.write("1896-1901\n")
                        elif year>=1902 and year <= 1906:
                            fout.write("1902-1906\n")
                        elif year>=1907 and year <= 1910:
                            fout.write("1907-1910\n")
                        elif year>=1911 and year < 1914:
                            fout.write("1911-1914\n")
                        elif year>=1915 and year <= 1918:
                            fout.write("1915-1918\n")
                        elif year>=1919 and year <= 1922:
                            fout.write("1919-1922\n")
                        else:
                            fout.write("1923-present\n")
                
if __name__ == '__main__':
	if len(sys.argv) != 2:
		print "Please provide doc_ID_Date file name."
	else:
		main(sys.argv[1])
