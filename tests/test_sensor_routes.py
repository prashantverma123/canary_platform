import json
import pytest
import sqlite3
import time
import unittest

from app import app


class SensorRoutesTestCases(unittest.TestCase):

    def setUp(self):
        # Setup the SQLite DB
        conn = sqlite3.connect('test_database.db')
        conn.execute('DROP TABLE IF EXISTS readings')
        conn.execute(
            'CREATE TABLE IF NOT EXISTS readings (device_uuid TEXT, type TEXT, value INTEGER, date_created INTEGER)')

        self.device_uuid = 'test_device'

        # Setup some sensor data
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'humidity', 22, int(time.time()) - 200))

        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 22, int(time.time()) - 100))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 50, int(time.time()) - 50))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 100, int(time.time())))

        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    ('other_uuid', 'temperature', 22, int(time.time())))
        conn.commit()

        app.config['TESTING'] = True

        self.client = app.test_client

    def test_device_readings_get(self):
        # Given a device UUID
        # When we make a request with the given UUID
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid))

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        # And the response data should have three sensor readings
        self.assertTrue(len(json.loads(request.data)) == 4)

    def test_device_readings_post(self):
        # Given a device UUID
        # When we make a request with the given UUID to create a reading
        request = self.client().post('/devices/{}/readings/'.format(self.device_uuid), data=
        json.dumps({
            'type': 'temperature',
            'value': 100
        }))

        # Then we should receive a 201
        self.assertEqual(request.status_code, 201)

        # And when we check for readings in the db
        conn = sqlite3.connect('test_database.db')
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('select * from readings where device_uuid="{}"'.format(self.device_uuid))
        rows = cur.fetchall()

        # We should have three
        self.assertTrue(len(rows) == 5)

    def test_device_readings_get_temperature(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's temperature data only.
        """
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid),
                                    query_string={"type": "temperature"})
        readings = json.loads(request.data)
        isAllTemperatureReadings = True
        for item in readings:
            if not item["type"] == "temperature":
                isAllTemperatureReadings = False
                break

        self.assertEqual(request.status_code, 200)
        self.assertTrue(isAllTemperatureReadings)

    def test_device_readings_get_humidity(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's humidity data only.
        """
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid), query_string={"type": "humidity"})
        readings = json.loads(request.data)
        isAllHumidityReadings = True
        for item in readings:
            if not item["type"] == "humidity":
                isAllHumidityReadings = False
                break

        self.assertEqual(request.status_code, 200)
        self.assertTrue(isAllHumidityReadings)

    def test_device_readings_get_past_dates(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's sensor data over
        a specific date range. We should only get the readings
        that were created in this time range.
        """
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid),
                                    query_string={"type": "temperature", "start": int(time.time()) - 50,
                                                  "end": int(time.time())})
        readings = json.loads(request.data)

        self.assertEqual(request.status_code, 200)
        self.assertEqual(len(readings), 2)

    def test_device_readings_max(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's max sensor reading.
        """
        request = self.client().get('/devices/{}/readings/max/'.format(self.device_uuid),
                                    query_string={"type": "temperature"})
        readings = json.loads(request.data)
        self.assertEqual(readings['value'], 100)

    def test_device_readings_median(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's median sensor reading.
        """
        request = self.client().get('/devices/{}/readings/median/'.format(self.device_uuid),
                                    query_string={"type": "temperature"})
        readings = json.loads(request.data)
        self.assertEqual(readings['value'], 50)

    def test_device_readings_mean(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's mean sensor reading value.
        """
        request = self.client().get('/devices/{}/readings/mean/'.format(self.device_uuid),
                                    query_string={"type": "temperature"})
        readings = json.loads(request.data)
        self.assertEqual(round(readings['value'], 2), 57.33)

    def test_device_readings_quartiles(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's 1st and 3rd quartile
        sensor reading value.
        """
        request = self.client().get('/devices/{}/readings/quartiles/'.format(self.device_uuid),
                                    query_string={"type": "temperature"})
        readings = json.loads(request.data)
        self.assertEqual(readings, {'quartile_1': 36.0, 'quartile_3': 75.0})

    def test_device_readings_summary(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's 1st and 3rd quartile
        sensor reading value.
        """
        request = self.client().get('/devices/summary/')
        readings = json.loads(request.data)
        expected_summary = [{'device_uuid': 'test_device', 'max_reading_value': 100, 'mean_reading_value': 48.5,
                             'median_reading_value': 36.0, 'number_of_readings': 4, 'quartile_1_value': 22.0,
                             'quartile_3_value': 62.5},
                            {'device_uuid': 'other_uuid', 'max_reading_value': 22, 'mean_reading_value': 22,
                             'median_reading_value': 22, 'number_of_readings': 1, 'quartile_1_value': 22.0,
                             'quartile_3_value': 22.0}]
        self.assertEqual(readings, expected_summary)
