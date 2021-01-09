from pymongo import MongoClient

from bson.objectid import ObjectId

import json

mongo_client = MongoClient('localhost', 27017)
db = mongo_client.NoSQL_311_Chicago_Incidents

def query2(start_date, end_date, type):
    res = db.incident.aggregate([
        {"$match": {"type": type}},
        {"$match": {"$expr": {"$and": [{"$gte": ["$creation_date", {"$dateFromString": {"dateString": start_date}}]},
                                       {"$lte": ["$creation_date", {"$dateFromString": {"dateString": end_date}}]}]}}},
        {"$group": {"_id": {"type": "$type", "creation_date": "$creation_date"},
                    "totalRequests": {"$sum": 1}}},
        {"$project": {"_id": 0, "creation_date": {"$dateToString": {"date": "$_id.creation_date"}}, "total_requests": "$totalRequests"}}
    ])
    return json.dumps(list(res))


