import csv
import json
import datetime
import pandas as pd
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


def incident_details(row):
    details = {}
    if 'Police District' in row:
        details['Police District'] = row['Police District']
        del row['Police District']
    if 'Community Area' in row:
        details['Community Area'] = row['Community Area']
        del row['Community Area']
    if 'Community Areas' in row:
        details['Community Areas'] = row['Community Areas']
        del row['Community Areas']
    if 'Historical Wards' in row:
        details['Historical Wards'] = row['Historical Wards']
        del row['Historical Wards']
    if 'Census Tracts' in row:
        details['Census Tracts'] = row['Census Tracts']
        del row['Census Tracts']
    if 'Zip Codes' in row:
        details['Zip Codes'] = row['Zip Codes']
        del row['Zip Codes']

    return (row, details)

def location_field(row):
    loc_field = {}
    loc_field['Street Address'] = row['Street Address']
    loc_field['Zip Code'] = row['Zip Code']
    loc_field['X Coordinate'] = row['X Coordinate']
    loc_field['Y Coordinate'] = row['Y Coordinate']
    loc_field['Latitude'] = row['Latitude']
    loc_field['Longitude'] = row['Longitude']

    return loc_field

def data_clean(row, fields, incident_type):
    if incident_type == "311-service-requests-pot-holes-reported":
        row['Type of Service Request'] = "Pothole in Street"
    if incident_type == "311-service-requests-street-lights-one-out":
        row['Type of Service Request'] = "Street Light Out"

    row['Location'] = location_field(row)
    del row['Street Address']
    del row['Zip Code']
    del row['X Coordinate']
    del row['Y Coordinate']
    del row['Latitude']
    del row['Longitude']

    if incident_type == "311-service-requests-rodent-baiting":
        del row['']

    row['Creation Date'] = datetime.datetime.strptime(row['Creation Date'], '%Y-%m-%dT%H:%M:%S.%f')
    if row['Completion Date'] != "":
        row['Completion Date'] = datetime.datetime.strptime(row['Completion Date'], '%Y-%m-%dT%H:%M:%S.%f')

    return row

for title in csv_file_titles:
    count = 0

    csvfile = open(datapath + title + '.csv', 'r')
    reader = csv.DictReader(csvfile)

    headers = reader.fieldnames

    for row in reader:
        if title == "311-service-requests-rodent-baiting":
            if row[''] != "":
                continue
        (row, details) = incident_details(row)
        inc = db.incident.insert_one(data_clean(row, headers, title))
        details['Incident_Id'] = format(inc.inserted_id)
        inc_details = db.incident_details.insert_one(details)
        count = count + 1
        if count > 5:
            break

    print("Inserted " + str(count) + " incidents from " + title + " CSV file")