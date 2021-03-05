import sys
from datetime import *

import re
from sqlalchemy import create_engine
from sqlalchemy.exc import DatabaseError
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
import os

from statics_from_config import *

reload(sys)
sys.setdefaultencoding('utf8')

db_user = DB_USER
db_password = DB_PASSWORD
db_name = DB_NAME
db_host = DB_HOST
port = 8080
connection_string = 'postgresql://{}:{}@{}:{}/{}'
connection_string = connection_string.format(db_user, db_password, db_host, port, db_name)
engine = None
current_sop = os.environ['SOP']
date = datetime.now()
cur_year = str(datetime.today().isocalendar()[0])[-2:]
if datetime.today().weekday() == 0:
    cur_week = str(datetime.today().isocalendar()[1])
else:
    cur_week = str(datetime.today().isocalendar()[1] + 1)
wk = "{0}CW{1}".format(cur_year, cur_week)
status_dictionary = {
    'Success': 1,
    'Failure': 2,
    'Error': 3,
    'Skipped': 4
}

reversed_status_dictionary = {
    1: 'Success',
    2: 'Failure',
    3: 'Error',
    4: 'Skipped'
}


def open_connection():
    global engine
    try:
        engine = create_engine(connection_string, client_encoding='utf8', echo=False)
    except DatabaseError, e:
        print "Error %s" % e
        sys.exit(1)
    return engine


def create_session():
    global engine
    if not engine:
        engine = open_connection()

    return Session(engine)


def add_rc_info(build_no, rc, week, sop, device_ip):
    try:
        try:
            session = create_session()
            Base = automap_base()
            Base.prepare(engine, reflect=True)
            Build_info = Base.classes.build_info

            build_info = Build_info(build_no=build_no, rc=rc, week=week, sop=sop, device_ip=device_ip)
            session.add(build_info)
            session.commit()
        except DatabaseError, e:
            print "Error %s" % e
            sys.exit(1)

        print('inserted value: ')
        try:
            query = session.query(Build_info).filter_by(build_no=build_no, rc=rc).first()
            print "Id: {}, Build no: {}, RC: {}, Week: {}, SOP: {}, Device IP: {}".format(query.id, query.build_no,
                                                                                          query.rc,
                                                                                          query.week, query.sop,
                                                                                          query.device_ip)
            if session:
                session.close()
        except DatabaseError, e:
            print "Error %s" % e
            sys.exit(1)
    except Exception, e:
        print "Error %s" % e
        sys.exit(1)


def add_build_summary(build_no, data=None):
    try:
        if data is not None:
            try:
                session = create_session()
                Base = automap_base()
                Base.prepare(engine, reflect=True)
                Build_info = Base.classes.build_info
                Result = Base.classes.results
                Fw_version = Base.classes.fw_versions
                Framework = Base.classes.frameworks
                Jtf_version = Base.classes.jtf_versions
                Jtf_test_package = Base.classes.jtf_test_packages
                Map_version = Base.classes.map_versions
                Jenkins_link = Base.classes.jenkins_links
                try:
                    build_id = session.query(Build_info).filter_by(build_no=build_no).first().id
                except AttributeError:
                    print "No rc info (in build_info table) added to DB yet. Please add it first"
                    sys.exit(1)
            except DatabaseError, e:
                print "Error %s" % e
                sys.exit(1)

            columns = data.keys()
            values = data.values()

            fw_version_index = columns.index("fw_version")
            framework_index = columns.index("framework")
            jtf_version_index = columns.index("jtf_version")
            jtf_test_package_index = columns.index("jtf_test_package")
            map_version_index = columns.index("map_version")
            jenkins_link_index = columns.index("jenkins_link")
            total_index = columns.index("total")
            success_index = columns.index("success")
            failure_index = columns.index("failure")
            error_index = columns.index("error")
            skip_index = columns.index("skip")

            try:
                already_exist = session.query(Jenkins_link).filter_by(link=values[jenkins_link_index]).first()
                if already_exist is not None:
                    print "Results by this jenkins link already exists in DB"
                    sys.exit(1)
                fw_version_actual = session.query(Fw_version).filter_by(version=values[fw_version_index]) \
                    .first()
                exist = fw_version_actual is not None

                if not exist:
                    fw_version = Fw_version(version=values[fw_version_index])
                    session.add(fw_version)
                    session.commit()
                    fw_version_actual = session.query(Fw_version).filter_by(version=values[fw_version_index]) \
                        .first()
                    print "Inserted {} in Fw_versions table".format(fw_version_actual.version)
                else:
                    print "Current Fw_version already exist in DB"
            except DatabaseError, e:
                print "Error %s" % e
                sys.exit(1)

            try:
                framework_actual = session.query(Framework).filter_by(framework=values[framework_index]) \
                    .first()
                exist = framework_actual is not None

                if not exist:
                    framework = Framework(framework=values[framework_index])
                    session.add(framework)
                    session.commit()
                    framework_actual = session.query(Framework).filter_by(framework=values[framework_index]) \
                        .first()
                    print "Inserted {} in Frameworks table".format(framework_actual.framework)
                else:
                    print "Current Framework already exist in DB"
            except DatabaseError, e:
                print "Error %s" % e
                sys.exit(1)

            try:
                jtf_version_actual = session.query(Jtf_version).filter_by(version=values[jtf_version_index]) \
                    .first()
                exist = jtf_version_actual is not None

                if not exist:
                    jtf_version = Jtf_version(version=values[jtf_version_index])
                    session.add(jtf_version)
                    session.commit()
                    jtf_version_actual = session.query(Jtf_version).filter_by(version=values[jtf_version_index]) \
                        .first()
                    print "Inserted {} in Jtf_versions table".format(jtf_version_actual.version)
                else:
                    print "Current Jtf_version already exist in DB"
            except DatabaseError, e:
                print "Error %s" % e
                sys.exit(1)

            try:
                jtf_test_package_actual = session.query(Jtf_test_package) \
                    .filter_by(package=values[jtf_test_package_index]).first()
                exist = jtf_test_package_actual is not None

                if not exist:
                    jtf_test_package = Jtf_test_package(package=values[jtf_test_package_index])
                    session.add(jtf_test_package)
                    session.commit()
                    jtf_test_package_actual = session.query(Jtf_test_package) \
                        .filter_by(package=values[jtf_test_package_index]).first()
                    print "Inserted {} in Jtf_test_packages table".format(jtf_test_package_actual.package)
                else:
                    print "Current Jtf_test_package already exist in DB"
            except DatabaseError, e:
                print "Error %s" % e
                sys.exit(1)

            try:
                map_version_actual = session.query(Map_version).filter_by(version=values[map_version_index]) \
                    .first()
                exist = map_version_actual is not None

                if not exist:
                    map_version = Map_version(version=values[map_version_index])
                    session.add(map_version)
                    session.commit()
                    map_version_actual = session.query(Map_version).filter_by(version=values[map_version_index]) \
                        .first()
                    print "Inserted {} in Map_versions table".format(map_version_actual.version)
                else:
                    print "Current Map_version already exist in DB"
            except DatabaseError, e:
                print "Error %s" % e
                sys.exit(1)

            try:
                jenkins_link_actual = session.query(Jenkins_link).filter_by(link=values[jenkins_link_index]) \
                    .first()
                exist = jenkins_link_actual is not None

                if not exist:
                    jenkins_link = Jenkins_link(link=values[jenkins_link_index])
                    session.add(jenkins_link)
                    session.commit()
                    jenkins_link_actual = session.query(Jenkins_link).filter_by(link=values[jenkins_link_index]) \
                        .first()
                    print "Current Jenkins_link already exist in DB"
                else:
                    print "Inserted {} in Jenkins_link table".format(jenkins_link_actual.link)
            except DatabaseError, e:
                print "Error %s" % e
                sys.exit(1)

            try:
                result = Result(build_id=build_id, total=values[total_index], success=values[success_index],
                                failure=values[failure_index], error=values[error_index], skip=values[skip_index],
                                date=date, fw_version_id=fw_version_actual.id, framework_id=framework_actual.id,
                                jtf_version_id=jtf_version_actual.id,
                                jtf_test_package_id=jtf_test_package_actual.id,
                                map_version_id=map_version_actual.id, jenkins_link_id=jenkins_link_actual.id)
                session.add(result)
                session.commit()
            except DatabaseError, e:
                print "Error %s" % e
                sys.exit(1)

            try:
                result_actual = session.query(Result).filter_by(date=date).first()
                if(result_actual is not None):
                    fw_version_in_results = session.query(Fw_version).join(Result) \
                        .filter(Fw_version.id == Result.fw_version_id).order_by(Result.date.desc()).first().version
                    framework_in_results = session.query(Framework).join(Result) \
                        .filter(Framework.id == Result.framework_id).order_by(Result.date.desc()).first().framework
                    jtf_version_in_results = session.query(Jtf_version).join(Result) \
                        .filter(Jtf_version.id == Result.jtf_version_id).order_by(Result.date.desc()).first().version
                    jtf_test_package_in_results = session.query(Jtf_test_package).join(Result) \
                        .filter(Jtf_test_package.id == Result.jtf_test_package_id).order_by(
                        Result.date.desc()).first().package
                    map_version_in_results = session.query(Map_version).join(Result) \
                        .filter(Map_version.id == Result.map_version_id).order_by(Result.date.desc()).first().version
                    jenkins_link_in_results = session.query(Jenkins_link).join(Result) \
                        .filter(Jenkins_link.id == Result.jenkins_link_id).order_by(Result.date.desc()).first().link

                    print "Inserted value in results table: Id = {}, Build_Id = {}, Total = {}, Success = {}, " \
                        "Failure = {}, Error = {}, Skip = {}, Date = {}, Fw_version = {}, Framework = {}, " \
                        "Jtf_version = {}, Jtf_test_package = {}, Map_version = {}, Jenkins_link = {}".format(
                        result_actual.id,
                        result_actual.build_id,
                        result_actual.total,
                        result_actual.success,
                        result_actual.failure,
                        result_actual.error,
                        result_actual.skip,
                        result_actual.date,
                        fw_version_in_results,
                        framework_in_results,
                        jtf_version_in_results,
                        jtf_test_package_in_results,
                        map_version_in_results,
                        jenkins_link_in_results)
                else:
                    print "Can not get value from results"

            except DatabaseError, e:
                print "Error %s" % e
                sys.exit(1)
        else:
            "No data parsed from Jenkins"
            sys.exit(1)
        if session:
            session.close()
    except Exception, e:
        print "Error %s" % e
        sys.exit(1)


def insert_test_data(jenkins_link, data_arr=None):
    try:
        try:
            session = create_session()
            Base = automap_base()
            Base.prepare(engine, reflect=True)
            Result = Base.classes.results
            Jenkins_link = Base.classes.jenkins_links
            Package = Base.classes.packages
            Test = Base.classes.tests

        except DatabaseError, e:
            print "Error %s" % e
            sys.exit(1)

        try:
            build_id = session.query(Result).join(Jenkins_link) \
                .filter(Result.jenkins_link_id == Jenkins_link.id and Jenkins_link.link == jenkins_link) \
                .order_by(Result.date.desc()).first().id

        except DatabaseError, e:
            print "Error %s" % e
            sys.exit(1)

        def get_column_indexes():
            for data in data_arr:
                column = data.keys()
                test_name_index = column.index("name")
                package_index = column.index("package")
                status_index = column.index("status")
                message_index = column.index("message")
                if test_name_index and package_index and status_index and message_index is not None:
                    break
                return test_name_index, package_index, status_index, message_index

        def get_packages():
            packages = []
            for data in data_arr:
                value = tuple(data.values())
                packages.append(value[package_index])
            return set(packages)

        test_name_index, package_index, status_index, message_index = get_column_indexes()
        packages = get_packages()

        try:
            for package in packages:
                exist = session.query(Package).filter_by(package=package).first()
                if not exist:
                    package_to_add = Package(package=package)
                    session.add(package_to_add)
                    print "Added new package {} to DB".format(package)
                else:
                    print "Package {} already exist in DB".format(package)
                    continue
            session.commit()
        except DatabaseError, e:
            print "Error %s" % e
            sys.exit(1)

        try:
            for data in data_arr:
                value = tuple(data.values())

                name = value[test_name_index]
                status = value[status_index]
                message = value[message_index]
                package = value[package_index]

                test_status = status_dictionary[status]
                package_id = session.query(Package).filter_by(package=package).first().id

                test = Test(name=name, package_id=package_id, result_id=build_id, status_id=test_status,
                            message=message, last_updated_date=date)
                session.add(test)
                session.commit()

                print "Info about test '{}' with result_id '{}' added to DB".format(name, build_id)

            if session:
                session.close()

        except DatabaseError, e:
            print "Error %s" % e
            sys.exit(1)

        print "Successfully updated tests info in DB"
    except Exception, e:
        print "Error %s" % e
        sys.exit(1)


def select_rc_id(week=None, build_no=None):
    try:
        try:
            session = create_session()
            Base = automap_base()
            Base.prepare(engine, reflect=True)
            Build_info = Base.classes.build_info

            fetched_rc_data = []

            if current_sop is not None:
                if (week is not None) and (build_no is None):
                    rows = session.query(Build_info).filter_by(week=week, sop=current_sop).order_by(
                        Build_info.rc.desc()).all()
                elif (week is None) and (build_no is not None):
                    rows = session.query(Build_info).filter_by(build_no=build_no, sop=current_sop) \
                        .order_by(Build_info.rc.desc()).all()
            else:
                print "No current sop version set in config_file.ini"

        except DatabaseError, e:
            print "Error %s" % e
            sys.exit(1)

        if len(rows) != 0:
            for row in rows:
                dict_rc = {
                    'id': row.id,
                    'build_no': row.build_no,
                    'rc': row.rc,
                    'week': row.week,
                    'sop': row.sop
                }
                fetched_rc_data.append(dict_rc)

    except Exception, e:
        print "Error %s" % e
        sys.exit(1)

    return fetched_rc_data


def select_rc_results(rc_id):
    try:
        try:
            session = create_session()
            Base = automap_base()
            Base.prepare(engine, reflect=True)
            Result = Base.classes.results
            Fw_version = Base.classes.fw_versions
            Framework = Base.classes.frameworks
            Jtf_version = Base.classes.jtf_versions
            Jtf_test_package = Base.classes.jtf_test_packages
            Map_version = Base.classes.map_versions
            Jenkins_link = Base.classes.jenkins_links

            row = session.query(Result).filter_by(build_id=rc_id).order_by(Result.date.desc()).first()

            fetched_rc_results_data = []

            dict_rc = {
                'build_no_id': row.build_id,
                'date_time': row.date,
                'error': row.error,
                'failure': row.failure,
                'framework': session.query(Framework).filter_by(id=row.framework_id).first().framework,
                'fw_version': session.query(Fw_version).filter_by(id=row.fw_version_id).first().version,
                'id': row.id,
                'jenkins_link': session.query(Jenkins_link).filter_by(id=row.jenkins_link_id).first()
                    .link,
                'jtf_test_package': session.query(Jtf_test_package).filter_by(id=row.jtf_test_package_id)
                    .first().package,
                'jtf_version': session.query(Jtf_version).filter_by(id=row.jtf_version_id).first().version,
                'map_version': session.query(Map_version).filter_by(id=row.map_version_id).first().version,
                'skip': row.skip,
                'success': row.success,
                'total': row.total
            }
            fetched_rc_results_data.append(dict_rc)
        except DatabaseError, e:
            print "Error %s" % e
            sys.exit(1)
    except Exception, e:
        print "Error %s" % e
        sys.exit(1)

    return fetched_rc_results_data


def fetch_test_data(run_id):
    try:
        try:
            session = create_session()
            Base = automap_base()
            Base.prepare(engine, reflect=True)
            Test = Base.classes.tests
            Status = Base.classes.statuses
            Result = Base.classes.results
            Package = Base.classes.packages

            results = []

            rows = session.query(Test).filter_by(result_id=run_id).all()

            if len(rows) == 0:
                print "\nNo test results stored, please run test and upload results\n"
            else:
                print "\nResults stored in DB"

                for row in rows:
                    test_status = reversed_status_dictionary[row.status_id]
                    package = session.query(Package).filter_by(id=row.package_id).first().package
                    results.append({
                        'name': row.name,
                        'status': test_status,
                        'package': package,
                        'message': row.message})

            return results

        except DatabaseError, e:
            print "Error %s" % e
            sys.exit(1)
    except Exception, e:
        print "Error %s" % e
        sys.exit(1)


def fetch_and_format_data(week=None, rc_build=None):
    pr_wk = None
    if week is None:
        week = wk
        pr_wk = "{0}CW{1}".format(cur_year, str(int(week.split('CW')[1]) - 1))

    formatted_data = []

    for item in select_rc_id(week):
        for run in select_rc_results(item['id']):
            rc_data = {
                "rc_data": item,
                "run_data": run,
                "tests_data": fetch_test_data(run['id'])
            }
            formatted_data.append(rc_data)

    if pr_wk:
        for item in select_rc_id(pr_wk):
            for run in select_rc_results(item['id']):
                rc_data = {
                    "rc_data": item,
                    "run_data": run,
                    "tests_data": fetch_test_data(run['id'])
                }
                formatted_data.append(rc_data)
    return formatted_data


def fetch_rc_data():
    p_wk = "{0}CW{1}".format(cur_year, int(cur_week) - 1)
    try:
        try:
            session = create_session()
            Base = automap_base()
            Base.prepare(engine, reflect=True)
            Build_info = Base.classes.build_info
            Result = Base.classes.results

            rows = session.query(Build_info, Result).join(Result).filter(Build_info.id == Result.build_id,
                                                                         Build_info.week == wk).all()
            rows += session.query(Build_info, Result).join(Result).filter(Build_info.id == Result.build_id,
                                                                          Build_info.week == p_wk).all()

            if len(rows) != 0:
                db_data = []
                for row in rows:
                    db_data.append({
                        "build_no": row.build_info.build_no,
                        "rc": row.build_info.rc,
                        "week": row.build_info.week,
                        "total": row.results.total,
                        "success": row.results.success,
                        "failure": row.results.failure,
                        "error": row.results.error,
                        "skip": row.results.skip})
                return db_data
            else:
                print "There are no data for previous week."

        except DatabaseError, e:
            print "Error %s" % e
            sys.exit(1)
    except Exception, e:
        print "Error %s" % e
        sys.exit(1)


def fetch_weekly_report_data():
    try:
        try:
            session = create_session()
            base = automap_base()
            base.prepare(engine, reflect=True)
            result_sql_table = base.classes.results
            build_info = base.classes.build_info

            week = session.query(build_info).filter_by(sop=current_sop).order_by(build_info.id.desc()).first().week
            rows = session.query(result_sql_table).join(build_info)\
                .filter(build_info.id == result_sql_table .build_id, build_info.sop == current_sop, build_info.week == week)\
                .all()
            weekly_result_data = []
            for row in rows:
                build_number = re.search('\d{6}', row.build_info.build_no).group()
                row_data = [row.build_info.rc + ' ' + build_number, row.total, row.success, row.failure + row.error]
                if row_data not in weekly_result_data:
                    weekly_result_data.append(row_data)
            print weekly_result_data
            return weekly_result_data
        except DatabaseError, e:
            print "Error %s" % e
            sys.exit(1)
    except Exception, e:
        print "Error %s" % e
        sys.exit(1)


if __name__ == '__main__':
    count_arguments = len(sys.argv) - 1
    if count_arguments == 4:
        if 'MAIN' in sys.argv[1]:
            add_rc_info(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
        else:
            add_rc_info(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])

    if count_arguments == 5:
        if 'MAIN' in sys.argv[1]:
            add_rc_info(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
        else:
            add_rc_info(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])

    else:
        print("Wrong parameters. Please use Build_No and RC, WK, SOP for adding RC to database")
        print("Like script.py MIB2P_MAIN_xx_xx_x_xxxxxx RCX 1xCWxx SOPx")

    sys.exit()
