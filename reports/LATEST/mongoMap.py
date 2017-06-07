from pymongo import MongoClient


client = MongoClient('10.2.42.205', 27017)
db = client.ecquant
all_stories = db.news.find({}).count()

print all_stories
