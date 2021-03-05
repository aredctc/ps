import os

from pymongo import MongoClient
from collections import defaultdict
import pandas as pd
import datetime
import json


#mongodb client
client = MongoClient('', 27017)
db = client['performance']
db.authenticate('', '')
collection = db['olp_performance']

test_name = os.environ['TESTNAME']

#The only appropriate way to store data in PerfViz is using columns,
# rows, names and values. So there are the constants
columns = 'columns'
rows = 'rows'
name = 'name'
values = 'values'
latency_dict = {}
test_results = {}
columns_latency = "minLatency", "maxLatency", "avgLatency"

#load data from statistics.json
with open('statistics.json') as json_file:
    data = json.load(json_file)

#load data from result_file.jtf (to fetch latency results)
performance_data_from_jtl = pd.read_csv("result_file.jtl")


def list_duplicates(seq):
    tally = defaultdict(list)
    for i,item in enumerate(seq):
        tally[item].append(i)
    return ((key,locs) for key,locs in tally.items())


def format_string_to_appropriate_for_mongodb(x):
    return x.replace(' ', '_').replace('\'', '').replace('.', '/')


def populate_latency_dict():
    label_list = []
    latency_list = []

    for label in performance_data_from_jtl["label"]:
        label_list.append(label)

    for latency in performance_data_from_jtl["Latency"]:
        latency_list.append(latency)


    for x, y in list_duplicates(label_list):
        values_array = []

        for z in y:
            values_array.append(latency_list[z])
        latency_dict[x] = values_array


#To provide unique sequence number for performance graph we need to check
#the last stored one and increment it
def get_seqNumber():
    last_test_result = collection.find_one({ 'name': test_name })
    if last_test_result is not None:
        seq_number = collection.find_one({ 'name': test_name },
                                         sort=[('creationTime', -1)])['seqNumber']
        return seq_number + 1
    else:
        print("No test data for this test recorded yet, seqNumber will be 1")
        return 1


def get_latency_values():
    populate_latency_dict()

    for x, y in latency_dict.items():
        values_array = []
        rows_array = []

        values_array.append(min(y))
        values_array.append(max(y))
        values_array.append(sum(y) / len(y))

        row = {
            name: "Latency",
            values: values_array
        }
        rows_array.append(row)

        values_dict = {
            columns : columns_latency,
            rows : rows_array
        }

        results_values_dict = {
            format_string_to_appropriate_for_mongodb(x) + "_latency" : values_dict
        }

        test_results.update(results_values_dict)
    print("Fetched values for latency statistic")


def get_response_time_values():
    for x, y in data.items():
        columns_array = []
        values_array = []
        rows_array = []

        for z, i in y.items():
            if "ResTime" not in z:
                continue

            columns_array.append(z)
            values_array.append(i)

        row = {
            name : "ResponseTime",
            values : values_array
        }
        rows_array.append(row)

        values_dict = {
            columns : columns_array,
            rows : rows_array
        }

        results_values_dict = {
            format_string_to_appropriate_for_mongodb(x) + "_responseTime" : values_dict
        }

        test_results.update(results_values_dict)
    print("Fetched values for Response time")


def get_error_statistic_values():
    for x, y in data.items():
        columns_array = []
        values_array = []
        rows_array = []

        for z, i in y.items():
            if ("errorCount" not in z) and ("sample" not in z):
                continue

            columns_array.append(z)
            values_array.append(i)

        row = {
            name : "Sample_error_statistic",
            values : values_array
        }
        rows_array.append(row)

        values_dict = {
            columns : columns_array,
            rows : rows_array
        }

        results_values_dict = {
            format_string_to_appropriate_for_mongodb(x) + "_sampleErrorStatistic" : values_dict
        }

        test_results.update(results_values_dict)
    print("Fetched values for Samples & errors statistic")


def get_throughput_values():
    for x, y in data.items():
        columns_array = []
        values_array = []
        rows_array = []

        for z, i in y.items():
            if "throughput" not in z:
                continue

            columns_array.append(z)
            values_array.append(i)

        row = {
            name : "Throughput",
            values: values_array
        }
        rows_array.append(row)

        values_dict = {
            columns: columns_array,
            rows : rows_array
        }

        results_values_dict = {
            format_string_to_appropriate_for_mongodb(x) + "_throughput" : values_dict
        }

        test_results.update(results_values_dict)
    print("Fetched values for throughput statistic")


def get_sent_and_received_kbs_values():
    for x, y in data.items():
        columns_array = []
        values_array = []
        rows_array = []

        for z, i in y.items():
            if "KBytesPerSec" not in z:
                continue

            columns_array.append(z)
            values_array.append(i)

        row = {
            name : "KBytesPerSec",
            values : values_array
        }
        rows_array.append(row)

        values_dict = {
            columns : columns_array,
            rows : rows_array
        }

        results_values_dict = {
            format_string_to_appropriate_for_mongodb(x) + "_KBytesPerSec" : values_dict
        }

        test_results.update(results_values_dict)
    print("Fetched values for sent and received KB sec statistic")


get_latency_values()
get_response_time_values()
get_error_statistic_values()
get_throughput_values()
get_sent_and_received_kbs_values()

data_to_insert = {
    'name' : test_name,
    'seqNumber' : get_seqNumber(),
    'branch' : 'test',
    'creationTime' : datetime.datetime.utcnow(),
    'results' : test_results
}

collection.insert_one(data_to_insert)

print(data_to_insert)