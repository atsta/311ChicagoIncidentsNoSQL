from flask import Flask, jsonify, request
from pymongo import MongoClient
from bson.objectid import ObjectId
import queries

app = Flask(__name__)

mongo_client = MongoClient('localhost', 27017)
db = mongo_client.NoSQL_311_Chicago_Incidents

@app.route('/query1', methods = ['GET'])
def _query1():
    _json = request.json
    _start_date = _json['Start Date']
    _end_date = _json['End Date']

    res = queries.query1(_start_date, _end_date)
    return res

@app.route('/query2', methods = ['GET'])
def _query2():
    _json = request.json
    _start_date = _json['Start Date']
    _end_date = _json['End Date']
    _type = _json['Type']

    res = queries.query2(_start_date, _end_date, _type)
    return res



if __name__ == '__main__':
    app.run(debug=True)
