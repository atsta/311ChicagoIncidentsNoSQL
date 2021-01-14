from pymongo import MongoClient
from bson.objectid import ObjectId
import json

mongo_client = MongoClient('localhost', 27017)
db = mongo_client.NoSQL_311_Chicago_Incidents
