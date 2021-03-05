import datetime
import re

import dominate
from dominate.tags import link, html, body, td
from dominate.util import raw
from html3.html3 import HTML

import main.utils.common_functionality
import main.utils.utils
import main.hdlm.hdlm
import main.fast_map.fast_map
import main.feature_confliation.feature_confliation
import generate_final_report


def get_table_header(html_end_point):
    """
        Generate table header
    """
    html_end_point.table(border='1')
    table_header = html_end_point.tr.th('Feature', rowspan='3').th('Checkpoint 1', rowspan='2', colspan='2') \
        .th('Checkpoint 2', colspan='5').th('Checkpoint 3', colspan='5').th('Checkpoint 4', colspan='5')
    table_header.tr.td('Layer', rowspan='2', id="head") \
        .th('Earliest traffic', colspan='2').th('Latest traffic', colspan='2').td('Layer', rowspan='2', id="head") \
        .th('Earliest traffic', colspan='2') \
        .th('Latest traffic', colspan='2').td('Layer', rowspan='2', id="head") \
        .th('Earliest traffic', colspan='2').th('Latest traffic', colspan='2')
    table_header.tr.td('abs. time', id="head").td('rel. time', id="head").td('abs. time', id="head") \
        .td('rel. time', id="head").td('abs. time', id="head").td('rel. time', id="head") \
        .td('abs. time', id="head").td('rel. time', id="head").td('abs. time', id="head").td('rel. time', id="head") \
        .td('abs. time', id="head").td('rel. time', id="head").td('abs. time', id="head").td('rel. time', id="head")
    return table_header


def get_feature_conflation_table_part(html_end_point, fc, list_fc, tile):
    """
        Generate feature conflation table part for received fc layer name
    """
    color_latest = main.feature_confliation.feature_confliation.get_if_latest_timestamp_is_actual(
        time=main.feature_confliation.feature_confliation.get_latest_timestamp_by_tile(list_fc[0], tile))
    feature_conflation_part = html_end_point.td(fc).td(main.utils.utils.convert_timestamp_to_date(
        main.feature_confliation.feature_confliation.get_earliest_timestamp_by_tile(list_fc[0], tile)),
        style="color:black;") \
        .td().b(main.utils.common_functionality.count_relative_time(
            main.feature_confliation.feature_confliation.get_earliest_timestamp_by_tile(list_fc[0], tile))).b \
        .td(main.utils.utils.convert_timestamp_to_date(
            main.feature_confliation.feature_confliation.get_latest_timestamp_by_tile(list_fc[0], tile)),
            style="color:" + color_latest + ";") \
        .td().b(
        main.utils.common_functionality.count_relative_time(
            main.feature_confliation.feature_confliation.get_latest_timestamp_by_tile(list_fc[0], tile))).b
    return feature_conflation_part


def get_fast_map_table_part(html_end_point, fm, list_fm, feature_name, tile):
    """
        Generate fast map table part for received fast map layer name
    """
    color_latest = main.fast_map.fast_map.get_if_latest_version_tile_fast_map_is_actual(
        time=main.utils.common_functionality.get_latest_version_by_tile(list_fm[0], tile),
        feature_name=feature_name,
        tile=tile)
    color_earliest = main.fast_map.fast_map.get_if_earliest_version_tile_fast_map_is_actual(
        time=main.utils.common_functionality.get_earliest_version_by_tile(list_fm[0], tile),
        feature_name=feature_name,
        tile=tile)
    fast_map_part = html_end_point.td(fm).td(
        main.utils.utils.convert_timestamp_to_date(
            main.utils.common_functionality.get_earliest_version_by_tile(list_fm[0], tile)),
        style="color:" + color_earliest + ";") \
        .td.b(
        main.utils.common_functionality.count_relative_time(
            main.utils.common_functionality.get_earliest_version_by_tile(list_fm[0], tile))).b \
        .td(main.utils.utils.convert_timestamp_to_date(
            main.utils.common_functionality.get_latest_version_by_tile(list_fm[0], tile)),
            style="color:" + color_latest + ";") \
        .td.b(main.utils.common_functionality.count_relative_time(
            main.utils.common_functionality.get_latest_version_by_tile(list_fm[0], tile))).b
    return fast_map_part


def get_hdlm_table_part(html_end_point, hdlm, list_hdlm, feature_name, tile):
    """
        Generate HDLM table part for received hdlm layer name
    """
    color_latest = main.hdlm.hdlm.get_if_latest_version_tile_hdlm_is_actual(
        time=main.utils.common_functionality.get_latest_version_by_tile(
            list_hdlm[0], tile),
        feature_name=feature_name,
        tile=tile)
    color_earliest = main.hdlm.hdlm.get_if_earliest_version_tile_hdlm_is_actual(
        time=main.utils.common_functionality.get_earliest_version_by_tile(
            list_hdlm[0],
            tile),
        feature_name=feature_name,
        tile=tile)
    hdlm_part = html_end_point.td(hdlm).td(
        main.utils.utils.convert_timestamp_to_date(
            main.utils.common_functionality.get_earliest_version_by_tile(list_hdlm[0], tile)),
        style="color:" + color_earliest + ";") \
        .td.b(
        main.utils.common_functionality.count_relative_time(
            main.utils.common_functionality.get_earliest_version_by_tile(list_hdlm[0], tile))).b \
        .td(main.utils.utils.convert_timestamp_to_date(
            main.utils.common_functionality.get_latest_version_by_tile(list_hdlm[0], tile)),
            style="color:" + color_latest + ";") \
        .td.b(
        main.utils.common_functionality.count_relative_time(
            main.utils.common_functionality.get_latest_version_by_tile(list_hdlm[0], tile))).b.tr()
    return hdlm_part


def generate_report(tile):
    """
        Generate final report.
        In a case some of a checkpoint part is absent empty rows is filled with '-----'
        and '-------------' for layer column.
        File generates to root/reports dir with the name tile.
        Gantt diagram is attached under main report.
        Links for each created detailed report attached under the gantt diagram
    """
    doc = dominate.document(title=tile)
    h = HTML()
    end_of_table = h.h1(
        datetime.datetime.fromtimestamp(generate_final_report.job_timestamp / 1000).__format__(
            generate_final_report.fmt) +
        " EEST (GMT+3), Kyiv started").h1
    h_header = get_table_header(h)
    for current_feature in main.utils.utils.get_list_of_features():
        feature_row = main.utils.common_functionality.get_features_table_part(h_header, current_feature)
        list_fc = main.feature_confliation.feature_confliation.get_list_of_feature_conflation_by_feature(
            current_feature)
        list_fm = main.fast_map.fast_map.get_list_of_fast_map_by_feature(current_feature)
        list_hdlm = main.hdlm.hdlm.get_list_of_HDLM_by_feature(current_feature)
        index = 0
        while not index == main.utils.common_functionality.count_rowspan_by_feature(current_feature):
            try:
                fc_row = get_feature_conflation_table_part(html_end_point=feature_row,
                                                           fc=re.findall('.{0,}_(.{0,}).json', list_fc[0])[0],
                                                           list_fc=list_fc, tile=tile)
                list_fc.remove(list_fc[0])
            except IndexError:
                fc_row = feature_row.td('-------------').td('-----').td('-----').td('-----').td('-----')
            try:
                fm_row = get_fast_map_table_part(html_end_point=fc_row,
                                                 fm=re.findall('.{0,}_(.{0,}).json', list_fm[0])[0],
                                                 list_fm=list_fm, feature_name=current_feature,
                                                 tile=tile)
                list_fm.remove(list_fm[0])
            except IndexError:
                fm_row = fc_row.td('-----------').td('-----').td('-----').td('-----').td('-----')
            try:
                end_of_table = get_hdlm_table_part(fm_row, re.findall('.{0,}_(.{0,}).json', list_hdlm[0])[0],
                                                   list_hdlm, current_feature, tile)
                list_hdlm.remove(list_hdlm[0])
            except IndexError:
                end_of_table = fm_row.td('-----------').td('-----').td('-----').td('-----').td('-----').tr()
            index += 1

    with doc.head:
        link(rel='stylesheet', href='style.css')

    with doc.body:
        str(html(body(td(raw(str(h))))))

    file = open('reports/' + tile + '.html', 'w+')
    file.write(str(doc))
