import json
import datetime
import sys, getopt, pprint
from pymongo import MongoClient
from processing_and_insertion import location_field, incident_details

mongo_client = MongoClient('localhost', 27017)
db = mongo_client.NoSQL_311_Chicago_Incidents

def insert_new(_json):

    (_json, details) = incident_details(_json)

    (_json, _json['location']) = location_field(_json)

    _json['creation_date'] = datetime.datetime.strptime(_json['creation_date'], '%Y-%m-%dT%H:%M:%S.%f')
    if _json['completion_date'] != "":
        _json['completion_date'] = datetime.datetime.strptime(_json['completion_date'], '%Y-%m-%dT%H:%M:%S.%f')

    _json['upvotes'] = []

    inc = db.incident.insert_one(_json)
    details['incident_Id'] = format(inc.inserted_id)
    db.incident_details.insert_one(details)

    return
