import statistics

from flask import Flask, render_template, request, Response
from flask.json import jsonify
import json
import sqlite3
import time

from schemas.ReadingsQuerySchema import GetReadingsQuerySchema, CreateReadingsQuerySchema, GetMaxReadingQuerySchema, \
    GetSummaryReadingQuerySchema
from utils.db import connect
from utils.query import get_readings, compute_summary
import numpy as np

app = Flask(__name__)

# Setup the SQLite DB
conn = sqlite3.connect('database.db')
conn.execute('CREATE TABLE IF NOT EXISTS readings (device_uuid TEXT, type TEXT, value INTEGER, date_created INTEGER)')
conn.close()


@app.route('/devices/<string:device_uuid>/readings/', methods=['POST', 'GET'])
def request_device_readings(device_uuid):
    """
    This endpoint allows clients to POST or GET data specific sensor types.

    POST Parameters:
    * type -> The type of sensor (temperature or humidity)
    * value -> The integer value of the sensor reading
    * date_created -> The epoch date of the sensor reading.
        If none provided, we set to now.

    Optional Query Parameters:
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    * type -> The type of sensor value a client is looking for
    """

    # Set the db that we want and open the connection
    cur, connection = connect(app)

    if request.method == 'POST':
        # Grab the post parameters
        validate_readings_schema = CreateReadingsQuerySchema().validate(json.loads(request.data))
        if validate_readings_schema:
            return {"error": validate_readings_schema},400
        post_data = json.loads(request.data)
        sensor_type = post_data.get('type')
        value = post_data.get('value')
        date_created = post_data.get('date_created', int(time.time()))

        # Insert data into db
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (device_uuid, sensor_type, value, date_created))

        connection.commit()

        # Return success
        return 'success', 201
    else:
        query_params = request.args.to_dict()
        validates_query_params = GetReadingsQuerySchema().validate(query_params)
        if validates_query_params:
            return {"error": validates_query_params}, 400
        query_params["device_uuid"] = device_uuid
        rows = get_readings(cur,"get",query_params)

        # Return the JSON
        return jsonify([dict(zip(['device_uuid', 'type', 'value', 'date_created'], row)) for row in rows]), 200


@app.route('/devices/<string:device_uuid>/readings/max/', methods=['GET'])
def request_device_readings_max(device_uuid):
    """
    This endpoint allows clients to GET the max sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    cur, connection = connect(app)

    query_params = request.args.to_dict()
    validates_query_params = GetMaxReadingQuerySchema().validate(query_params)
    if validates_query_params:
        return {"error": validates_query_params}, 400

    query_params["device_uuid"] = device_uuid
    rows = get_readings(cur, "max", query_params)

    # Return the JSON
    return {"value": rows[0]["value"]}, 200



@app.route('/devices/<string:device_uuid>/readings/median/', methods=['GET'])
def request_device_readings_median(device_uuid):
    """
    This endpoint allows clients to GET the median sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    cur, connection = connect(app)

    query_params = request.args.to_dict()
    validates_query_params = GetMaxReadingQuerySchema().validate(query_params)
    if validates_query_params:
        return {"error": validates_query_params}, 400

    query_params["device_uuid"] = device_uuid
    rows = get_readings(cur, "median", query_params)

    value_list = []
    for row in rows:
        value_list.append(row["value"])

    # Return the JSON
    return {"value":statistics.median(value_list)}, 200


@app.route('/devices/<string:device_uuid>/readings/mean/', methods=['GET'])
def request_device_readings_mean(device_uuid):
    """
    This endpoint allows clients to GET the mean sensor readings for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    cur, connection = connect(app)

    query_params = request.args.to_dict()
    validates_query_params = GetMaxReadingQuerySchema().validate(query_params)
    if validates_query_params:
        return {"error": validates_query_params}, 400

    query_params["device_uuid"] = device_uuid
    rows = get_readings(cur, "mean", query_params)

    value_list = []
    for row in rows:
        value_list.append(row["value"])

    # Return the JSON
    return {"value": statistics.mean(value_list)}, 200


@app.route('/devices/<string:device_uuid>/readings/quartiles/', methods=['GET'])
def request_device_readings_quartiles(device_uuid):
    """
    This endpoint allows clients to GET the 1st and 3rd quartile
    sensor reading value for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    cur, connection = connect(app)
    query_params = request.args.to_dict()
    validates_query_params = GetMaxReadingQuerySchema().validate(query_params)
    if validates_query_params:
        return {"error": validates_query_params}, 400

    query_params["device_uuid"] = device_uuid
    rows = get_readings(cur, "mean", query_params)
    value_list = []
    for row in rows:
        value_list.append(row["value"])

    return {'quartile_1': np.quantile(value_list, .25),'quartile_3': np.quantile(value_list, .75)}, 200


@app.route('/devices/summary/', methods=['GET'])
def request_readings_summary():
    """
    This endpoint allows clients to GET a full summary
    of all sensor data in the database per device.

    Optional Query Parameters
    * type -> The type of sensor value a client is looking for
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    cur, connection = connect(app)
    query_params = request.args.to_dict()
    validates_query_params = GetSummaryReadingQuerySchema().validate(query_params)
    if validates_query_params:
        return {"error": validates_query_params}, 400

    return jsonify(compute_summary(cur,query_params)), 200


if __name__ == '__main__':
    app.run()
