import datetime
import json
from os import listdir
import re

from gantt import Gantt

import generate_final_report


def get_time_from_timestamp(timestamp):
    return datetime.datetime.fromtimestamp(int(timestamp) / 1000)


def get_latest_timestamp_overall():
    max_timestamps = []
    for current in listdir(generate_final_report.artifacts_path):
        if current.__contains__('featureConflation'):
            max_list = []
            data = open(generate_final_report.artifacts_path + '/' + current).read()
            fc_json = json.loads(data)
            fc_list = fc_json['listOfFeatureConflations']
            for current_timestamp in fc_list:
                max_list.append(max(current_timestamp['timestamps']))
            max_timestamps.append(max(max_list))
        elif current.__contains__('.json'):
            latest_timestamp = 0
            data = open(generate_final_report.artifacts_path + '/' + current).read()
            layer_json = json.loads(data)
            list_of_partitions = layer_json['listOfPartitionPerformance']
            for current_list_of_maps in list_of_partitions:
                if current_list_of_maps['lastVersion'] is not None and \
                        latest_timestamp < float(current_list_of_maps['lastVersion']):
                    latest_timestamp = float(current_list_of_maps['lastVersion'])
            max_timestamps.append(latest_timestamp)
    return max(max_timestamps)


def get_run_hours(start_timestamp, end_timestamp):
    start = get_time_from_timestamp(start_timestamp)
    end = get_time_from_timestamp(end_timestamp)
    value = end - start
    return round(value.total_seconds() / 3600, 2)


def get_feature_conflation():
    max_fc_list = []
    min_fc_list = []
    fc_packages = []
    i = 0
    for current in listdir(generate_final_report.artifacts_path):
        if current.__contains__('featureConflation'):
            data = open(generate_final_report.artifacts_path + '/' + current).read()
            fc_json = json.loads(data)
            fc_list = fc_json['listOfFeatureConflations']
            layer_name = re.findall('.*_(.*).json', current)[0]
            max_list = []
            min_list = []
            if not len(data) == 0:
                for current_timestamp in fc_list:
                    if not get_time_from_timestamp(min(current_timestamp['timestamps'])) \
                           < get_time_from_timestamp(generate_final_report.job_timestamp):
                        max_list.append(max(current_timestamp['timestamps']))
                        min_list.append(min(current_timestamp['timestamps']))
                        max_fc_list.append(max(current_timestamp['timestamps']))
                        min_fc_list.append(min(current_timestamp['timestamps']))
            if len(min_list) == 0:
                layer_values = {
                    'label': layer_name,
                    'start': get_run_hours(generate_final_report.job_timestamp, generate_final_report.job_timestamp),
                    'end': get_run_hours(generate_final_report.job_timestamp,generate_final_report.job_timestamp),
                    'color': 'orange'
                }
            elif not get_time_from_timestamp(min(min_list)) < get_time_from_timestamp(
                    generate_final_report.job_timestamp):
                if i < 1:
                    layer_values = {
                        'label': layer_name,
                        'start': get_run_hours(generate_final_report.job_timestamp, min(min_list)),
                        'end': get_run_hours(generate_final_report.job_timestamp, (max(max_list))),
                        'color': 'orange',
                        'legend': 'Layer'
                    }
                    fc_packages.append(layer_values)
                    i += 1
                else:
                    layer_values = {
                        'label': layer_name,
                        'start': get_run_hours(generate_final_report.job_timestamp, min(min_list)),
                        'end': get_run_hours(generate_final_report.job_timestamp, (max(max_list))),
                        'color': 'orange'
                    }
                    fc_packages.append(layer_values)
    if not min_fc_list or not max_fc_list:
        return []
    else:
        fc = {
            'label': 'Feature conflation',
            'start': get_run_hours(generate_final_report.job_timestamp, min(min_fc_list)),
            'end': get_run_hours(generate_final_report.job_timestamp, max(max_fc_list)),
            'legend': 'Subsystem'
        }
        fc_packages.insert(0, fc)
        return fc_packages


def get_fast_map():
    earliest_list = []
    latest_list = []
    fm_packages = []
    for current in listdir(generate_final_report.artifacts_path):
        if current.__contains__('fastMap'):
            data = open(generate_final_report.artifacts_path + '/' + current).read()
            layer_json = json.loads(data)
            list_of_fast_maps = layer_json['listOfPartitionPerformance']
            layer_name = re.findall('.*_(.*).json', current)[0]
            if not len(list_of_fast_maps) == 0:
                earliest = float(list_of_fast_maps[0]['earliestVersion'])
                latest = float(list_of_fast_maps[0]['lastVersion'])
                for current_partition in list_of_fast_maps:
                    if current_partition['earliestVersion'] is None or current_partition['lastVersion'] is None:
                        break
                    if earliest > float(current_partition['earliestVersion']):
                        earliest = float(current_partition['earliestVersion'])
                    if latest < float(current_partition['lastVersion']):
                        latest = float(current_partition['lastVersion'])
                if not get_time_from_timestamp(earliest) < get_time_from_timestamp(generate_final_report.job_timestamp):
                    earliest_list.append(earliest)
                    latest_list.append(latest)
                    layer_values = {
                        'label': layer_name,
                        'start': get_run_hours(generate_final_report.job_timestamp, earliest),
                        'end': get_run_hours(generate_final_report.job_timestamp, latest),
                        'color': 'orange'
                    }
                    fm_packages.append(layer_values)
    if not earliest_list or not latest_list:
        return []
    else:
        fm = {
            'label': 'FastMap',
            'start': get_run_hours(generate_final_report.job_timestamp, min(earliest_list)),
            'end': get_run_hours(generate_final_report.job_timestamp, max(latest_list))
        }
        fm_packages.insert(0, fm)
        return fm_packages


def get_hdlm():
    earliest_list = []
    latest_list = []
    hdlm_packages = []
    for current in listdir(generate_final_report.artifacts_path):
        if current.__contains__('HDLM'):
            data = open(generate_final_report.artifacts_path + '/' + current).read()
            layer_json = json.loads(data)
            list_of_hdlm = layer_json['listOfPartitionPerformance']
            layer_name = re.findall('.*_(.*).json', current)[0]
            if not len(list_of_hdlm) == 0:
                earliest = float(list_of_hdlm[0]['earliestVersion'])
                latest = float(list_of_hdlm[0]['lastVersion'])
                for current_partition in list_of_hdlm:
                    if float(current_partition['earliestVersion']) is None or float(current_partition['lastVersion']
                                                                                    is None):
                        break
                    if earliest > float(current_partition['earliestVersion']):
                        earliest = float(current_partition['earliestVersion'])
                    if latest < float(current_partition['lastVersion']):
                        latest = float(current_partition['lastVersion'])
                if not get_time_from_timestamp(earliest) < get_time_from_timestamp(generate_final_report.job_timestamp):
                    earliest_list.append(earliest)
                    latest_list.append(latest)
                    layer_values = {
                        'label': layer_name,
                        'start': get_run_hours(generate_final_report.job_timestamp, earliest),
                        'end': get_run_hours(generate_final_report.job_timestamp, latest),
                        'color': 'orange'
                    }
                    hdlm_packages.append(layer_values)
    if not earliest_list or not latest_list:
        return []
    else:
        hdlm = {
            'label': 'HDLM',
            'start': get_run_hours(generate_final_report.job_timestamp, min(earliest_list)),
            'end': get_run_hours(generate_final_report.job_timestamp, max(latest_list))
        }
        hdlm_packages.insert(0, hdlm)
        return hdlm_packages


def insert_jenkins_job_time():
    jenkins_job_packages = []
    jenkins_job_values = {
        'label': 'Jenkins job',
        'start': 0,
        'end': 1,
        'color': 'lightgreen',
        "milestones": [1],
        'legend': 'Jenkins job'
    }
    jenkins_job_packages.append(jenkins_job_values)

    return jenkins_job_packages


def initialize_packages():
    packages = insert_jenkins_job_time() + get_feature_conflation() + get_fast_map() + get_hdlm()
    return packages


def split_overall_time():
    splited_arr = []
    total_hours = get_run_hours(generate_final_report.job_timestamp, get_latest_timestamp_overall())
    part_value = total_hours / 19
    i = 0
    while i < 20:
        splited_arr.append(round(i * part_value, 2))
        i += 1
    return splited_arr


def initialize_json():
    json_structure = {
        'packages': initialize_packages(),
        'title': 'Gantt for Wall-E Performance testing',
        'xlabel': 'time (hours)',
        'xticks': split_overall_time()
    }
    with open('sample.json', 'w') as outfile:
        json.dump(json_structure, outfile)


def initialize_gantt():
    initialize_json()
    g = Gantt('./sample.json')
    g.render()
    # for debug
    # g.show()
    g.save('reports/GANTT.png')
