import cx_Oracle
import os
import json
import time
import argparse


def parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', dest='config', default='awr_config.json',
                        help='file config, default awr_config.json')
    parser.add_argument('--from_time', dest='from_time', default=None, help='example format "26/05/20 '
                                                                            '09:22"')
    parser.add_argument('--to_time', dest='to_time', default=None, help='example format "26/05/20 09:22"')
    return parser.parse_args()


def db_connect(cfg):
    username = cfg['cfg']['username']
    password = cfg['cfg']['password']
    data_basename = cfg['cfg']['data_basename']
    my_connection = cx_Oracle.connect(username, password, data_basename)
    return my_connection


def sql_exuetor(connect, sql_line):
    # Cоздаем курсор
    my_cursor = connect.cursor()
    # выполяем sql-запрос
    my_cursor.execute(sql_line)
    return my_cursor


def main():
    t0 = time.time()

    args = parse()

    cfg = json.load(open(args.config, "r"))

    if args.from_time and args.to_time is not None:
        cfg['cfg']['from'] = args.from_time
        cfg['cfg']['to'] = args.to_time

    my_connection = db_connect(cfg)
    print("Database version:", my_connection.version)

    sql_query_from = r"SELECT  DBID,snap_id, to_char(end_interval_time, 'dd/mm/yy hh24:mi') FROM dba_hist_snapshot " \
                     "where to_char(end_interval_time, 'dd/mm/yy hh24:mi') = '%s' ORDER BY snap_id desc " % (
                         cfg['cfg']['from'])

    result = sql_exuetor(my_connection, sql_query_from).fetchall()

    for (DBID, snap_id_from, time_snap_from) in result:
        print('%s | %s | %s' % (DBID, snap_id_from, time_snap_from))

    sql_query_to = r"SELECT  DBID,snap_id, to_char(end_interval_time, 'dd/mm/yy hh24:mi') FROM dba_hist_snapshot " \
                   "where to_char(end_interval_time, 'dd/mm/yy hh24:mi') = '%s' ORDER BY snap_id desc " % (
                       cfg['cfg']['to'])

    result = sql_exuetor(my_connection, sql_query_to).fetchall()

    for (DBID, snap_id_to, time_snap_to) in result:
        print('%s | %s | %s' % (DBID, snap_id_to, time_snap_to))

    sql_query_awr = r"SELECT * FROM TABLE(sys.DBMS_WORKLOAD_REPOSITORY." \
                    "awr_report_html(%s,1,%s,%s))" % (DBID, snap_id_from, snap_id_to)

    result = sql_exuetor(my_connection, sql_query_awr).fetchall()
    # print(result)
    filename = 'awr.html'
    if not os.path.exists(os.path.join(cfg['cfg']['path_dir'])):
        os.mkdir(os.path.join(cfg['cfg']['path_dir']))
    with open(os.path.join(cfg['cfg']['path_dir'], filename), 'w', encoding='utf-8') as file:
        for i in result:
            if i[0] is not None:
                file.writelines(i[0])
            # print(i[0])
            # print('%s = %s' % (type(i[0]), i[0]))
    print('awr done: %s' % (os.path.join(cfg['cfg']['path_dir'], filename)))
    my_connection.close
    print('Duration = %s sec' % (time.time() - t0))


if __name__ == '__main__':
    main()
