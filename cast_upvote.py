from flask import Flask, jsonify, request
from pymongo import MongoClient
from bson.objectid import ObjectId

mongo_client = MongoClient('localhost', 27017)
db = mongo_client.NoSQL_311_Chicago_Incidents

def cast_upvote(_citizen_id, incident_id):
    _incident_id = ObjectId(incident_id)
    _citizen_id = ObjectId(_citizen_id)

    if not db.incident.find_one({"_id": _incident_id}):
        resp = jsonify("Incident with id: " + _incident_id + " not fount")
        return resp
    elif not db.citizen.find_one({"_id": _citizen_id}):
        resp = jsonify("Citizen with id: " + _citizen_id + " not fount")
        return resp
    else:
        # check limit of upvotes for this citizen
        res = db.citizen.aggregate([
            {"$match": {"_id": _citizen_id}},
            {"$project": {"_id": 0, "upvotes": {"$size": "$upvotes"}}}
        ])

        num_of_upvotes = list(res)[0].get("upvotes")

        if num_of_upvotes >= 1000:
            resp = jsonify("Citizen has upvoted more 1000 incidents. Cannot add more upvotes.")
            return resp

        # check if citizen has already upvoted this incident
        res = db.citizen.aggregate([
            {"$match": {"_id": _citizen_id}},
            {"$project": {"has_id": {"$in": [_incident_id, "$upvotes"]}}}
        ])

        if list(res)[0].get('has_id'):
            resp = jsonify("Citizen has already upvoted this incident.")
            return resp

        # actual upvoting
        res = db.citizen.aggregate([
            {"$match": {"_id": _citizen_id}},
            {"$project": {"_id": 0, "cit": {"name": "$name", "phone": "$phone"}}}
        ])

        cit = list(res)[0].get('cit')
        cit_name = cit['name']
        cit_phone = cit['phone']

        new_upvote = {'name': cit_name, "phone": cit_phone}

        db.incident.update_one({"_id": _incident_id}, {"$push": {"upvotes": new_upvote}})
        db.citizen.update_one({"_id": _citizen_id}, {"$push": {"upvotes": _incident_id}})

        resp = jsonify("New upvote has been casted for citizen: " + cit_name)
        return resp