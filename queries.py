from pymongo import MongoClient

from bson.objectid import ObjectId

import json

mongo_client = MongoClient('localhost', 27017)
db = mongo_client.NoSQL_311_Chicago_Incidents

def query2(start_date, end_date, type):
    res = db.incident.aggregate([
        {"$group": {"_id": {"type": "$type"}, "total": {"$sum": 1}}},
        {"$match": {"$expr": {"$eq":["$_id.type", "Abandoned Vehicle Complaint"]}}},
        {"$match": {"$expr": {"$and": [{"$gte": ["$creation_date", {"$dateFromString": {"dateString": "2015-04-07"}}]}, {"$lte": ["$_id.creation_date",{"$dateFromString": {"dateString": "2019-04-30"}}]}]}}},
        {"$project": {"creation_date": "$_id.creation_date", "total": "$total", "_id": 0}}
    ])
    return json.dumps(list(res))
