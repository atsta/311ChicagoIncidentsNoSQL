from flask import Flask, jsonify, request
from pymongo import MongoClient
from bson.objectid import ObjectId
from insert_new_incident import insert_new
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

@app.route('/query3', methods = ['GET'])
def _query3():
    _json = request.json
    _date = _json['Date']
    res = queries.query3(_date)
    return res

@app.route('/query4', methods = ['GET'])
def _query4():
    _json = request.json
    _type = _json['Type']
    res = queries.query4(_type)
    return res

@app.route('/query5', methods = ['GET'])
def _query5():
    _json = request.json
    _start_date = _json['Start Date']
    _end_date = _json['End Date']
    res = queries.query5(_start_date, _end_date)
    return res

@app.route('/query6', methods = ['GET'])
def _query6():
    _json = request.json
    _date = _json['Date']
    _bottom_limit = _json['Bottom']
    _upper_limit = _json['Upper']
    res = queries.query6(_date, _bottom_limit, _upper_limit)
    return res

@app.route('/query7', methods = ['GET'])
def _query7():
    _json = request.json
    _date = _json['Date']
    res = queries.query7(_date)
    return res

@app.route('/query8', methods = ['GET'])
def _query8():
    res = queries.query8()
    return res

@app.route('/query9', methods = ['GET'])
def _query9():
    res = queries.query9()
    return res

@app.route('/query10', methods = ['GET'])
def _query10():
    res = queries.query10()
    return res

@app.route('/query11', methods = ['GET'])
def _query11():
    _json = request.json
    _name = _json['Name']
    res = queries.query11(_name)
    return res

@app.errorhandler(404)
def not_found(error=None):
    resp = jsonify("Not Found " + request.url)
    resp.status_code = 404
    return resp

@app.route('/insert', methods = ['POST'])
def _insert_incident():
    _json = request.json
    _type = _json['type']

    if _type and request.method == 'POST':
        insert_new(_json)
        resp = jsonify("New incident of type " + _type + " added successfully")
        return resp
    else:
        return not_found()

if __name__ == '__main__':
    app.run(debug=True)

