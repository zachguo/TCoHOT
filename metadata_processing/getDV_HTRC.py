#! /usr/bin/python

# Generate dependent variable in MongoDB.
# The collection name of extracted dependent variable is "dv".
# After running downloadMetadata.py, you will get a folder contains zip files. 
# An example folder name is: /path/to/xmlzips/
# To run: python getDV_HTRC.py /path/to/xmlzips/

from pymongo import MongoClient
import zipfile,xml2json,json,os,re,sys

def unzip2mongo(file,db):
    zf = zipfile.ZipFile(file, 'r')
    for fn in zf.namelist():
        try:
            js = json.loads(xml2json.xml2json(zf.read(fn)))
            for e in js["collection"]["record"]["controlfield"]:
                if e["_tag"]=="008":
                    if e["#text"][35:38]=="eng":
                        date = e["#text"][7:15].strip()
                        doc = {"_id":fn[:-4], "date": date}
                        db.dv.insert(doc)
        except KeyError:
            print 'ERROR: Did not find %s in zip file' % fn
       
                
def main(foldername):
    foldername = foldername.rstrip('/')
    client = MongoClient('localhost', 27017)
    db = client.HTRC
    collections = db.collection_names()
    if "dv" in collections:
        print "Collection 'dv' already exists in 'HTRC' database."
    else:
        for file in os.listdir(foldername):
            if(file.endswith(".zip")):
                zipfilepath = foldername+'/'+file
                unzip2mongo(zipfilepath,db)
                print "Successfully added into MongoDB:HTRC:dv ", zipfilepath


if __name__ == '__main__':
	if len(sys.argv) != 2:
		print "Please provide zip folder name."
	else:
		main(sys.argv[1])
