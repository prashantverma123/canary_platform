import statistics

import numpy as np

GET_ALL_READINGS = 'select * from readings where device_uuid="{}"'

GET_MAX_READINGS = 'select MAX(value) as value from readings where device_uuid="{}"'

GET_READINGS_VALUES = 'select value from readings where device_uuid="{}"'

GET_SUMMARY = 'SELECT MAX(value) as max_reading_value, COUNT(*) as number_of_readings, device_uuid ,GROUP_CONCAT(value) as value_list FROM readings GROUP BY device_uuid ORDER BY COUNT(*) DESC'

QUERY_MAP = {
    "get": GET_ALL_READINGS,
    "max": GET_MAX_READINGS,
    "median": GET_READINGS_VALUES,
    "mean": GET_READINGS_VALUES
}


def add_filters(query, query_params):
    if query_params.get("type"):
        query = query + ' AND type="{}"'.format(query_params.get("type"))
    if query_params.get("start"):
        query = query + ' AND date_created >={}'.format(query_params.get("start"))
    if query_params.get("end"):
        query = query + ' AND date_created <={}'.format(query_params.get("end"))
    return query


def get_readings(cursor, query_type, query_params):
    query = add_filters(QUERY_MAP[query_type].format(query_params.get("device_uuid")),query_params)
    cursor.execute(query)
    return cursor.fetchall()


def compute_summary(cursor, query_params):
    query = add_filters(GET_SUMMARY,query_params)
    cursor.execute(query)
    devices = cursor.fetchall()
    summary_list = []
    for device in devices:
        value_list = list(map(int, device["value_list"].split(",")))
        summary_list.append({
            "device_uuid": device["device_uuid"],
            "number_of_readings": device["number_of_readings"],
            "max_reading_value": device["max_reading_value"],
            "median_reading_value": statistics.median(value_list),
            "mean_reading_value": statistics.mean(value_list),
            'quartile_1_value': np.quantile(value_list, .25),
            'quartile_3_value': np.quantile(value_list, .75)
        })
    return summary_list
