import urllib2, zipfile, sys, os

URL_TEMPLATE = "http://chinkapin.pti.indiana.edu:9994/solr/MARC/?volumeIDs="
BATCH_SIZE = 80

class FileRequest:
    def __init__(self):
        self.numfound = 0
        self.volumeList = []
    
    def getVolumeIds(self, filename):
        with open(filename, 'r') as f:
            self.volumeList = [id.strip('\n') for id in f.readlines()]
        self.numfound = len(self.volumeList)

def generateURL(idlist):
	# concatenate volume id with pipe '|'
	return URL_TEMPLATE + '|'.join(idlist)

def downloadZIP(url, outfilename):
	f = urllib2.urlopen(url)
	with open(outfilename, 'w') as fout:
		chunk = f.read()
		while chunk:
			fout.write(chunk)
			chunk = f.read()

def main():        
    if (len(sys.argv) < 3):
        print ("python downloadMetadata.py <id-filename> <zip filepath>")
        sys.exit()
    else:
        idfilename = str(sys.argv[1])
        zipfilepath = str(sys.argv[2]).rstrip('/')+'/'
        if not os.path.isdir(zipfilepath):
            print ("Invalid or non-exist <zip filepath>")
            sys.exit()
    
    # load volume id from file
    idRequest = FileRequest()
    idRequest.getVolumeIds(idfilename)

    # exit if no volume id is returned
    print("Number of volumes read: " + str(len(idRequest.volumeList)))
    print("Number of volumes found: " + str(idRequest.numfound))
    if (len(idRequest.volumeList) == 0):
        print("No volume is returned from file. Please double check and try again.")
        sys.exit()
    # send batch request to DATA API
    start = 0
    length = len(idRequest.volumeList)
    while (start < length):
        print "Finish downloading ", start, " IDs."
        idlist = idRequest.volumeList[start : (start + BATCH_SIZE)]
        try:
            downloadZIP(generateURL(idlist), zipfilepath+'zip'+str(start)+'.zip')
        except IOError:
        	print generateURL(idlist)
        start = start + BATCH_SIZE
        

if __name__ == '__main__':
    main()
