
from pymongo import MongoClient
from bson.code import Code


connection = MongoClient()
db = connection.test

db.texts.remove()

#Insert the data 
lines = open('/home/ae/PycharmProjects/reports/mapreduce/2009-Obama.txt').readlines()
[db.texts.insert({'text': line}) for line in lines]

# Load map and reduce functions
mapper = Code("""
function wordMap(){
var words = this.text.match(/\w+/g);

					if (words == null){
						return;

					}


					for (var i = 0; i < words.length; i++){
						//emit every word, with count of one
						emit(words[i], {count: 1});

					}


				}
""")
reducer = Code("""
function wordReduce(key,values){

	var total = 0;
	for (var i = 0; i < values.length; i++){
		total += values[i].count;

	}

	return {count: total};


}


""")


#Run the map-reduce query
results = db.texts.map_reduce(mapper, reducer, "myresults")

#Print the results
for result in results.find():
	print result['_id'] , result['value']['count']
	
