#! /usr/bin/env python

# Generate dependent variable in MongoDB.
# The collection name of extracted dependent variable is "date".
# After running downloadMetadata.py, you will get a folder contains zip files. 
# An example folder name is: /path/to/xmlzips/
# To run: python getDV.py /path/to/xmlzips/

# Created by Bin Dai & Siyuan Guo, Mar 2014.

from pymongo import MongoClient
import zipfile,xml2json,json,os,re,sys

def date2daterange(year):
    if year <= 1839:
        return "pre-1839"
    elif year >= 1840 and year <= 1860:
        return "1840-1860"
    elif year >= 1861 and year <= 1876:
        return "1861-1876"
    elif year >= 1877 and year <= 1887:
        return "1877-1887"
    elif year >= 1888 and year <= 1895:
        return "1888-1895"
    elif year >= 1896 and year <= 1901:
        return "1896-1901"
    elif year >= 1902 and year <= 1906:
        return "1902-1906"
    elif year >= 1907 and year <= 1910:
        return "1907-1910"
    elif year >= 1911 and year <= 1914:
        return "1911-1914"
    elif year >= 1915 and year <= 1918:
        return "1915-1918"
    elif year >= 1919 and year <= 1922:
        return "1919-1922"
    else:
        return "1923-present"

def unzip2mongo(file, db):
    docs = []
    zf = zipfile.ZipFile(file, 'r')
    for fn in zf.namelist():
        try:
            js = json.loads(xml2json.xml2json(zf.read(fn)))
            for e in js["collection"]["record"]["controlfield"]:
                if e["_tag"]=="008":
                    if e["#text"][35:38]=="eng":
                        date = e["#text"][7:15].strip() # raw date with length of 8
                        shortdate = date[:4]
                        if shortdate.isdigit():
                            daterange = date2daterange(int(shortdate))
                        else:
                            daterange = 'Invalid'
                        docs.append({"_id":fn[:-4], "raw": date, "range": daterange})
        except KeyError:
            print 'ERROR: Did not find %s in zip file' % fn
    zf.close()
    db.date.insert(docs)
       
                
def main(foldername):
    foldername = foldername.rstrip('/')
    client = MongoClient('localhost', 27017)
    db = client.HTRC
    # Need not to check when inserting all volumes
    # collections = db.collection_names()
    # if "date" in collections:
    #     print "Collection 'date' already exists in 'HTRC' database. Drop it."
    #     db.drop_collection('date')
    for file in os.listdir(foldername):
        if(file.endswith(".zip")):
            zipfilepath = foldername+'/'+file
            unzip2mongo(zipfilepath,db)
            print "Successfully added into MongoDB:HTRC:date ", zipfilepath


if __name__ == '__main__':
	if len(sys.argv) != 2:
		print "Please provide zip folder name."
	else:
		main(sys.argv[1])
