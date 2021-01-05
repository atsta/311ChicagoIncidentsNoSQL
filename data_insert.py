import csv
import json
import pandas as pd
import sys, getopt, pprint
from pymongo import MongoClient

datapath = '/home/atstam/Documents/Master CS/Database Systems/Project 2/Data/'
csv_title = '311-service-requests-alley-lights-out'

csvfile = open(datapath + csv_title + '.csv', 'r')
reader = csv.DictReader(csvfile)

mongo_client = MongoClient('localhost', 27017)
db = mongo_client.NoSQL_311_Chicago_Incidents

alley_lights_headers = ["Creation Date", "Status", "Completion Date", "Service Request Number", "Type of Service Request", "Street Address", "ZIP Code", "X Coordinate",	"Y Coordinate", "Ward",	"Police District",	"Community Area",	"Latitude",	"Longitude",	"Location",	"Historical Wards 2003-2015",	"Zip Codes",	"Community Areas",	"Census Tracts",	"Wards"]

#alley_lights_fields = [creation_date,status,completion_date,service_request_number,type_of_service_request, street_address,zip_code,x_coordinate,y_coordinate,ward,police_district,community_area, latitude,longitude,location,historical_wards,zip_codes,community_areas,census_tracts,wards
'''
for each in reader:
    row={}
    for field in alley_lights_headers :
        row[field]=each[field]
    db.incident.insert_one(row)
'''
#delete all documents of incidents collection
db.incident.delete_many({ })