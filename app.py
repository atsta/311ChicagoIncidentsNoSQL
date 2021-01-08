from flask import Flask, jsonify, request

from pymongo import MongoClient

from bson.objectid import ObjectId

app = Flask(__name__)

mongo_client = MongoClient('localhost', 27017)
db = mongo_client.NoSQL_311_Chicago_Incidents

@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/query2', methods = ['GET'])
def _query2():
    _json = request.json
    _start_date = _json['Start Date']
    _end_date = _json['End Date']
    _type = _json['Type']

    print(_start_date, _end_date, _type)

    return 'Hello World!'



if __name__ == '__main__':
    app.run(debug=True)
