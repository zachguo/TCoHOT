#! /usr/bin/env python

# Get basic statistics of metadata, used for first presentation
# Created by Siyuan Guo, Feb 2014.

from collections import defaultdict
import matplotlib.pyplot as plt
import re, sys

METADATA_PATH_XML = "/Users/syg/Documents/corpora/hathitrust/metadata/non_google_pd_pdus.xml"
METADATA_PATH_HATHIFILE = "/Users/syg/Documents/corpora/hathitrust/metadata/hathi_full_20140201.txt"

class HathiYear:

	def __init__(self, infilename, datasource, checkIfValid=False, includeInvalid=True):
		if datasource == "H":
			self.yfd = self.getYearFreqDictFromHathifile(infilename, checkIfValid, includeInvalid)
		elif datasource == "X":
			self.yfd = self.getYearFreqDictFromXML(infilename)
		else:
			print "Wrong datasource format, should be either 'H'(Hathifile) or 'X'(XML). "

	def getYearFreqDictFromXML(self, infilename):
		yfd = defaultdict(int)
		countDoc = 0
		countDate = 0
		with open(infilename) as fin:
			for line in fin:
				if line:
					line = line.strip()
					if line.startswith("<record id="):
						countDoc += 1
					if line.startswith("<dc:date>"):
						matched = re.findall("(?<=\<dc\:date\>).*(?=\<\/dc\:date\>)", line)
						countDate += 1
						if len(matched)!=1:
							print line
						else:
							yfd[matched[0]]+=1
		print "# of Documents: ", countDoc
		print "# of Publication Date: ", countDate
		return yfd

	def getYearFreqDictFromHathifile(self, infilename, checkIfValid, includeInvalid):
		yfd = defaultdict(int)
		with open(infilename) as fin:
			for line in fin:
				if line:
					line = line.strip('\n')
					year = line.split('\t')[16]
					if not checkIfValid:
						yfd[year]+=1
					else:
						if year.isdigit():
							if int(year)>2014:
								if includeInvalid:
									yfd["2014+"]+=1
							else:
								yfd[int(year)]+=1
						elif includeInvalid:
							yfd[year]+=1
		print "# of Documents: ", sum(yfd.values())
		print "# of Missing Publication Date: ", yfd[""]
		print "# of Invalid Publication Date: ", yfd["2014+"]
		print "# of Valid Publication Date: ", sum(yfd.values())-yfd[""]-yfd["2014+"]
		return yfd

	def outputYearFreq(self, outfilename, sortByFreq=False):
		items = self.yfd.items()
		if not sortByFreq:
			items.sort(key=lambda x:x[0])
		else:
			items.sort(key=lambda x:x[1])
		with open(outfilename, "w") as fout:
			for date,freq in items:
				fout.write(str(date)+'\t'+str(freq)+'\n')


	def plotYearFreq(self):
		plt.bar(self.yfd.keys(), self.yfd.values(), align='center')
		plt.xlim([0,2015])
		plt.xlabel("Year")
		plt.ylabel("Number of Documents")
		plt.show()

def main():
	if len(sys.argv) < 2:
		print "Missing Argument: H or X, please indicate the file format of metadata source, X is XML and H is Hathifile."
		sys.exit()
	if sys.argv[1] not in "HX":
		print "Invalid Argument: H or X, please indicate the file format of metadata source, X is XML and H is Hathifile."
		sys.exit()
	elif sys.argv[1]=="H":
		hathiyear = HathiYear(METADATA_PATH_HATHIFILE, "H", True, True)
		hathiyear.outputYearFreq("output/yearfreqHathifile.txt")
	else:
		hathiyear = HathiYear(METADATA_PATH_XML, "X")
		hathiyear.outputYearFreq("output/yearfreqXML.txt", True)	

if __name__ == '__main__':
	main()