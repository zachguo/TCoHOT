#! /usr/bin/python

from collections import defaultdict
import re, sys

METADATA_PATH_XML = "/Users/syg/Documents/corpora/hathitrust/metadata/non_google_pd_pdus.xml"
METADATA_PATH_HATHIFILE = "/Users/syg/Documents/corpora/hathitrust/metadata/hathi_full_20140201.txt"

class HathiLang:

	def __init__(self, infilename, datasource):
		self.infilename = infilename
		if datasource == "H":
			self.lfd = self.getLangFreqDictFromHathifile()
		elif datasource == "X":
			self.lfd = self.getLangFreqDictFromXML()
		else:
			print "Wrong datasource format, should be either 'H'(Hathifile) or 'X'(XML). "

	def getLangFreqDictFromHathifile(self):
		lfd = defaultdict(int)
		with open(self.infilename) as fin:
			for line in fin:
				if line:
					line = line.strip('\n')
					lang = line.split('\t')[18]
					lfd[lang]+=1
		print "# of Documents: ", sum(lfd.values())
		print "# of Missing Language Tags: ", lfd[""]
		print "# of Language Tags: ", sum(lfd.values())-lfd[""]
		return lfd

	def getLangFreqDictFromXML(self):
		lfd = defaultdict(int)
		countDoc = 0
		countLang = 0
		with open(self.infilename) as fin:
			for line in fin:
				if line:
					line = line.strip()
					if line.startswith("<record id="):
						countDoc += 1
					if line.startswith("<dc:language>"):
						matched = re.findall("(?<=\<dc\:language\>).*(?=\<\/dc\:language\>)", line)
						countLang += 1
						if len(matched)!=1:
							print line
						else:
							lfd[matched[0]]+=1
		print "# of Documents: ", countDoc
		print "# of Language Tags: ", countLang
		return lfd

	def outputLangFreq(self, outfilename):
		lfl = sorted(self.lfd.items(), key=lambda x:x[1])
		with open(outfilename, "w") as fout:
			for lang,freq in lfl:
				fout.write(lang+"\t"+str(freq)+"\n")

def main():
	if len(sys.argv) < 2:
		print "Missing Argument: H or X, please indicate the file format of metadata source, X is XML and H is Hathifile."
		sys.exit()
	elif sys.argv[1] not in "HX":
		print "Invalid Argument: H or X, please indicate the file format of metadata source, X is XML and H is Hathifile."
		sys.exit()
	elif sys.argv[1]=="H":
		hathilang = HathiLang(METADATA_PATH_HATHIFILE, "H")
		hathilang.outputLangFreq("output/langfreqHathifile.txt")
	else:
		hathilang = HathiLang(METADATA_PATH_XML, "X")
		hathilang.outputLangFreq("output/langfreqXML.txt")

if __name__ == '__main__':
	main()
	