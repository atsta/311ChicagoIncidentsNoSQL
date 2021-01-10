from pymongo import MongoClient

from bson.objectid import ObjectId

import json

mongo_client = MongoClient('localhost', 27017)
db = mongo_client.NoSQL_311_Chicago_Incidents

def query1(start_date, end_date):
    res = db.incident.aggregate([
        {"$match": {"$expr": {"$and": [{"$gte": ["$creation_date", {"$dateFromString": {"dateString": start_date}}]},
                                       {"$lte": ["$creation_date", {"$dateFromString": {"dateString": end_date}}]}]}}},
        {"$group": {"_id": {"type":"$type", "creation_date": "$creation_date"}, "count": {"$sum": 1}}},

        {"$project": {"_id": 0, "creation_date": {"$dateToString": {"date": "$_id.creation_date"}}, "type": "$_id.type", "count": "$count"}},
        {"$sort": {"count": -1}}
    ])

    return json.dumps(list(res))

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

def query3(date):
    res = db.incident.aggregate([
        {"$match": {"$expr": {"$eq": ["$creation_date", {"$dateFromString": {"dateString": date}}]}}},
        {"$group": {"_id": {"zip": "$location.zip_code", "type": "$type"}, "total_occurences": {"$sum": 1}}},
        {"$sort": {"total_occurences": -1}},
        {"$group": {"_id": "$_id.zip", "zip_total": {"$push": {"type": "$_id.type", "total_occurences": "$total_occurences"}}}},
        {"$project": {"_id": 0, "zip_code": "$_id", "most_common": {"zip_total":{"$slice":["$zip_total", 0, 3]}}}}
    ])
    return json.dumps(list(res))

def query4(type):
    res = db.incident.aggregate([
        {"$match": {"type": type}},
        {"$group": {"_id": {"ward": "$ward"}, "count": {"$sum": 1}}},
        {"$sort": {"count": 1}},
        {"$project": {"_id": 0, "ward": "$_id.ward", "count": "$count"}},
        {"$limit": 3}
    ])
    return json.dumps(list(res))

def query5(start_date, end_date):
    res = db.incident.aggregate([
        {"$match": {"$expr": {"$and": [{"$ne": ["$completion_date", ""]},
                                       {"$gte": ["$creation_date", {"$dateFromString": {"dateString": start_date}}]},
                                        {"$lte": ["$creation_date", {"$dateFromString": {"dateString": end_date}}]}]}}},
        {"$group": {"_id": {"type": "$type"}, "duration":{"$avg": {"$subtract": ["$completion_date", "$creation_date"]}}}},
        {"$project": {"_id": 0, "type": "$_id.type", "average_duration":"$duration"}}
    ])
    return json.dumps(list(res))

