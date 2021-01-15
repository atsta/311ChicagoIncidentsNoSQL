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
        {"$project": {"_id": 0, "creation_date": {"$dateToString": {"date": "$_id.creation_date"}},
                      "total_requests": "$totalRequests"}}
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

def query6(date, bottom, upper):
    res = db.incident.aggregate([
        {"$match": {"location.coords": {"$geoWithin": {"$box": [bottom, upper]}}}},
        {"$match": {"$expr":  {"$eq": ["$creation_date", {"$dateFromString": {"dateString": date}}]}}},
        {"$group": {"_id": {"type": "$type"}, "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1},
        {"$project": {"_id": 0, "most_common_type": "$_id.type", "occurrences": "$count"}}
    ])
    return json.dumps(list(res))

def query7(date):
    res = db.incident.aggregate([
        {"$match": {"$expr": {"$eq": ["$creation_date", {"$dateFromString": {"dateString": date}}]}}},
        {"$project": {"_id": {"$toString": "$_id"}, "total_upvotes": {"$size": "$upvotes"}}},
        {"$sort": {"total_upvotes": -1}},
        {"$limit": 50}
    ])
    return json.dumps(list(res))

def query8():
    res = db.citizen.aggregate([
        {"$project": {"_id": 0, "name":"$name", "total_upvotes": {"$size": "$upvotes"}}},
        {"$sort": {"total_upvotes": -1}},
        {"$limit": 50}
    ])
    return json.dumps(list(res))

def query9():
    res = db.incident.aggregate([
        {"$match": {"$expr": {"$gte": [{"$size": "$upvotes"}, 1]}}},
        {"$project": {"_id": 1, "ward": 1, "upvotes": 1}},
        {"$unwind": "$upvotes"},
        {"$group": {"_id": {"cit": {"$concat": ["$upvotes.name"," - ","$upvotes.phone"]}, "ward": "$ward"}}},
        {"$group": {"_id": "$_id.cit", "upvoted": {"$sum": 1}}},
        {"$sort": {"upvoted": -1}},
        {"$limit": 50},
        {"$project": {"_id": 0, "citizen": "$_id", "upvoted_wards": "$upvoted"}}
    ])
    return json.dumps(list(res))

def query10():
    res = db.citizen.aggregate([
        {"$group": {"_id": "$phone", "citizens": {"$sum": 1}, "upvotes": {"$push":"$upvotes"}}},
        {"$match": {"citizens": {"$gte": 2}}},
        {"$unwind": "$upvotes"},
        {"$unwind": "$upvotes"},
        {"$group": {"_id": {"phone": "$_id", "incidents": "$upvotes"}, "same_tel": {"$sum": 1}}},
        {"$match": {"same_tel": {"$gte": 2}}},
        {"$project": {"_id": 0, "incident_id": {"$toString": "$_id.incidents"}}}
    ])
    return json.dumps(list(res))

def query11(_name):
    res = db.citizen.aggregate([
        {"$match": {"name": _name}},
        {"$lookup": {"from": "incident", "localField": "upvotes", "foreignField": "_id", "as": "upvotes"}},
        {"$project": {"_id": 0,"wards": "$upvotes.ward"}}
    ])
    return json.dumps(list(res))

