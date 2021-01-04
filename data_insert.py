import csv
import json
import pandas as pd
import sys, getopt, pprint
from pymongo import MongoClient

csvfile = open('C://test//final-current.csv', 'r')
reader = csv.DictReader(csvfile)

mongo_client = MongoClient('localhost', 27017)
db = mongo_client.NoSQL_311_Chicago_Incidents

db.segment.drop()
header= ["S No", "Instrument Name", "Buy Price", "Buy Quantity", "Sell Price", "Sell Quantity", "Last Traded Price", "Total Traded Quantity", "Average Traded Price", "Open Price", "High Price", "Low Price", "Close Price", "V" ,"Time"]

for each in reader:
    row={}
    for field in header:
        row[field]=each[field]
    db.incident.insert(row)