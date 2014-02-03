#!/usr/bin/python

# @para: the number of column you wanna extract; the output filename
def getCol(col, filename):
	with open(filename, "w") as fout:
		with open("hathi_full_20140201.txt") as fin:
			for line in fin:
				if line:
					line = line.strip()
					cols = line.split('\t')
					fout.write(cols[0]+'\t'+cols[col]+'\n') #16 is year, #18 is language

if __name__ == '__main__':
	getCol(16,"idyear.txt")
	# getCol(18,"idlanguage.txt")