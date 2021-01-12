import csv
import json
import datetime
import pandas as pd
from faker import Faker
import sys, getopt, pprint
from pymongo import MongoClient

mongo_client = MongoClient('localhost', 27017)
db = mongo_client.NoSQL_311_Chicago_Incidents

datapath = '/home/atstam/Documents/Master CS/Database Systems/Project 2/Data/'

csv_file_titles = ["311-service-requests-abandoned-vehicles",
                    "311-service-requests-alley-lights-out",
                    "311-service-requests-garbage-carts",
                    "311-service-requests-graffiti-removal",
                    "311-service-requests-pot-holes-reported",
                    "311-service-requests-rodent-baiting",
                    "311-service-requests-sanitation-code-complaints",
                    "311-service-requests-street-lights-all-out",
                    "311-service-requests-street-lights-one-out",
                    "311-service-requests-tree-debris",
                    "311-service-requests-tree-trims"]

db.incident.delete_many({})
db.incident_details.delete_many({})
db.citizen.delete_many({})

fake = Faker()
Faker.seed(0)

def incident_details(row):
    details = {}
    if 'police_district' in row:
        details['police_district'] = row['police_district']
        del row['police_district']
    if 'community_area' in row:
        details['community_area'] = row['community_area']
        del row['community_area']
    if 'community_areas' in row:
        details['community_areas'] = row['community_areas']
        del row['community_areas']
    if 'historical_wards' in row:
        details['historical_wards'] = row['historical_wards']
        del row['historical_wards']
    if 'census_tracts' in row:
        details['census_tracts'] = row['census_tracts']
        del row['census_tracts']
    if 'zip_codes' in row:
        details['zip_codes'] = row['zip_codes']
        del row['zip_codes']

    return (row, details)

def location_field(row):
    loc_field = {}
    loc_field['street_address'] = row['street_address']
    loc_field['zip_code'] = row['zip_code']
    loc_field['x_coordinate'] = row['x_coordinate']
    loc_field['y_coordinate'] = row['y_coordinate']
    loc_field['coords'] =  {
        "type" : "Point",
		"coordinates" : [
			float(row['lognitude']),
			float(row['latitude'])
		]
	}

    del row['street_address']
    del row['zip_code']
    del row['x_coordinate']
    del row['y_coordinate']
    del row['latitude']
    del row['lognitude']

    return (row, loc_field)

def data_clean(row, fields, incident_type):
    if incident_type == "311-service-requests-pot-holes-reported":
        row['type'] = "Pothole in Street"
    if incident_type == "311-service-requests-street-lights-one-out":
        row['type'] = "Street Light Out"

    (row, row['location']) = location_field(row)

    if incident_type == "311-service-requests-rodent-baiting":
        del row['']

    row['creation_date'] = datetime.datetime.strptime(row['creation_date'], '%Y-%m-%dT%H:%M:%S.%f')
    if row['completion_date'] != "":
        row['completion_date'] = datetime.datetime.strptime(row['completion_date'], '%Y-%m-%dT%H:%M:%S.%f')

    row['upvotes'] = []

    return row

#incidents insert
uniq = []
for title in csv_file_titles:
    count = 0

    csvfile = open(datapath + title + '.csv', 'r')
    reader = csv.DictReader(csvfile)
    headers = reader.fieldnames
    for col in headers:
        if col not in uniq:
            uniq.append(col)

    for row in reader:
        if title == "311-service-requests-rodent-baiting":
            if row[''] != "":
                continue
        (row, details) = incident_details(row)
        inc = db.incident.insert_one(data_clean(row, headers, title))
        details['incident_Id'] = format(inc.inserted_id)
        inc_details = db.incident_details.insert_one(details)
        count = count + 1
        if count > 15:
            break

    print("Inserted " + str(count) + " incidents from " + title + " CSV file")

print(uniq)

#citizen insert
n = 10
for i in range(n):
    citizen_info = {"name": fake.name(), "phone": fake.phone_number(), "address": fake.address(), "upvotes": []}
    result = db.citizen.insert_one(citizen_info)

print("Inserted " + str(n) + " citizens using Faker")

