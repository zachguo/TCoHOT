/* 
Generate dependent variable in MongoDB.
You should have successfully imported metadata JSON into MongoDB first.
The collection name of metadata source is assumed to be "metadata".
The collection name of extracted dependent variable is "dv".
To run this script, run command: mongo localhost:27017/HTRC getDV.js
*/

// function to clean up the "date" field of metadata
var cleanDate = function(str){
	if (str) {
		if (str.match(/^\D*(\d{4}).*$/)) {
			return str.replace(/^\D*(\d{4}).*$/, '$1'); // only keep the first seen 4 consecutive digits
		} else {
			return "ERROR:"+str;
		}
	} else {
		return "";
	}
}

// extract only ids and dates of English documents, clean up each date and insert into dv collection
var documents = db.metadata.find({"language":"eng"}, {"date":true});
db.dv.drop(); // make a fresh start
documents.forEach(
	function(e){
		if (e["date"]) {
			e["date"] = cleanDate(e["date"]);
		}
		db.dv.insert(e);
	}
);

// show number of empty, nonexistent, empty&nonexistent, erroneous and valid dates
// db.dv.find({"date":""}).count();
// db.dv.find({"date":{$exists:false}}).count();
// db.dv.find({$or:[{"date":""},{"date":{$exists:false}}]}).count();
// db.dv.find({"date":{$regex:/^ERROR:/}}).count();
// db.dv.find({"date":{$regex:/^\d{4}/}}).count();