import csv
import json
import datetime
import sys, getopt, pprint
from pymongo import MongoClient

mongo_client = MongoClient('localhost', 27017)
db = mongo_client.NoSQL_311_Chicago_Incidents

citizens = list(db.citizen.find({}))
number_of_incidents = db.incident.count_documents({})
upvoted_incidents = 0

while upvoted_incidents < number_of_incidents/3:
    for i in range(len(citizens)):
        upvote_dict = {
            'name': citizens[i]['name'],
            'phone': citizens[i]['phone'],
            'citizen_id': citizens[i]['_id']
        }
        random_incident = list(db.incident.aggregate([{"$sample": {"size": 1}}]))

        if len(random_incident[0]['upvotes']) == 0:
            upvoted_incidents = upvoted_incidents + 1
            db.incident.update_one({"_id": random_incident[0]['_id']}, {"$push": {"upvotes": upvote_dict}})
            db.citizen.update_one({"_id": citizens[i]['_id']}, {"$push": {"upvotes": random_incident[0]['_id']}})
        else:
            if random_incident[0]['_id'] not in citizens[i]['upvotes']:
                db.incident.update_one({"_id": random_incident[0]['_id']}, {"$push": {"upvotes": upvote_dict}})
                db.citizen.update_one({"_id": citizens[i]['_id']}, {"$push": {"upvotes": random_incident[0]['_id']}})
            else:
                continue

print(str(upvoted_incidents) + " incidents upvoted by citizens")
