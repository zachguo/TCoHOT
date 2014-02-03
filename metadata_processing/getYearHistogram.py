#! /usr/bin/python
from collections import defaultdict
import matplotlib.pyplot as plt
import re

class HathiYear:

	def __init__(self, infilename="idyear.txt", checkIfValid=False, includeInvalid=True, datasource):
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
					year = line.split('\t')[1]
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

	def outputYearFreq(self, filename="yearfreq.txt", sortByFreq=False):
		items = self.yfd.items()
		if not sortByFreq:
			items.sort(key=lambda x:x[0])
		else:
			items.sort(key=lambda x:x[1])
		with open(filename, "w") as fout:
			for date,freq in items:
				fout.write(str(date)+'\t'+str(freq)+'\n')


	def plotYearFreq(self):
		plt.bar(self.yfd.keys(), self.yfd.values(), align='center')
		plt.xlim([0,2015])
		plt.xlabel("Year")
		plt.ylabel("Number of Documents")
		plt.show()


if __name__ == '__main__':
	hathiyear = HathiYear("non_google_pd_pdus.xml", False, True, "X")
	# hathiyear = HathiYear("idyear.txt", True, True, "H")
	hathiyear.outputYearFreq("yearfreqXML.txt", True)