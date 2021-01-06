import csv
import json
import datetime
import pandas as pd
import sys, getopt, pprint
from pymongo import MongoClient

mongo_client = MongoClient('localhost', 27017)
db = mongo_client.NoSQL_311_Chicago_Incidents

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

def data_clean(row, fields, incident_type):
    if incident_type == "311-service-requests-pot-holes-reported":
        row['Type of Service Request'] = "Pothole in Street"
    if incident_type == "311-service-requests-street-lights-one-out":
        row['Type of Service Request'] = "Street Light Out"

    del row['Location']

    if incident_type == "311-service-requests-rodent-baiting":
        del row['']

    row['Creation Date'] = datetime.datetime.strptime(row['Creation Date'], '%Y-%m-%dT%H:%M:%S.%f')
    if row['Completion Date'] != "":
        row['Completion Date'] = datetime.datetime.strptime(row['Completion Date'], '%Y-%m-%dT%H:%M:%S.%f')

    return row

datapath = '/home/atstam/Documents/Master CS/Database Systems/Project 2/Data/'

for title in csv_file_titles:
    count = 0

    csvfile = open(datapath + title + '.csv', 'r')
    reader = csv.DictReader(csvfile)

    headers = reader.fieldnames

    for row in reader:
        #print(row)
        if title == "311-service-requests-rodent-baiting":
            if row[''] != "":
                continue
        db.incident.insert_one(data_clean(row, headers, title))
        count = count + 1

    print("Inserted " + str(count) + " incidents from " + title + " CSV file")