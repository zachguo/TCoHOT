#! /usr/bin/python
from collections import defaultdict
import re

class HathiLang:

	def __init__(self, infilename, outfilename, datasource):
		self.infilename = infilename
		self.outfilename = outfilename
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
					lfd[line.strip()]+=1
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

	def outputLangFreq(self):
		lfl = sorted(self.lfd.items(), key=lambda x:x[1])
		with open(self.outfilename, "w") as fout:
			for lang,freq in lfl:
				fout.write(lang+"\t"+str(freq)+"\n")


if __name__ == '__main__':
	# hathilang = HathiLang("language.txt", "languagefreq.txt", "H")
	hathilang = HathiLang("non_google_pd_pdus.xml", "languagefreqXML.txt", "X")
	hathilang.outputLangFreq()