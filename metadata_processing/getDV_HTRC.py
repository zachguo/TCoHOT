#! /usr/bin/python

# Generate dependent variable in MongoDB.
# The collection name of extracted dependent variable is "dv".
# After running downloadMetadata.py, you will get a folder contains zip files. For example folder name: /Users/thevenin/Documents/xml
# To run: python getDV.py /Users/thevenin/Documents/xml

from pymongo import MongoClient
import zipfile,xml2json,json,os,re, sys

def unzip(file,db):
    # filename is file name of the folder file
    zf = zipfile.ZipFile(file, 'r')
    for fn in zf.namelist():
        try:
            data = zf.read(fn)
            # every data is the text of an xml
            s = xml2json.xml2json_HTRC(data)
            js = json.loads(s)
            for e in js["collection"]["record"]["controlfield"]:
                if e["_tag"]=="008":
                    if e["#text"][35:38]=="eng":
                        date = e["#text"][7:15]
                        doc = {"_id":fn[:-4], "date": date}
                        db.dv.insert(doc)
        except KeyError:
            print 'ERROR: Did not find %s in zip file' % fn
       
                
def main(filename):
    client = MongoClient('localhost', 27017)
    db = client.HTRC
    for file in os.listdir(filename):
        if(file.endswith(".zip")):
            zipfilepath = filename+'/'+file
            unzip(zipfilepath,db)


if __name__ == '__main__':
	if len(sys.argv) != 2:
		print "Please provide zip folder filename."
	else:
		main(sys.argv[1])
