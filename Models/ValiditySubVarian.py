import time
start_time = time.time()

import json
import great_expectations as ge
import pymysql
import pymysql.cursors
import pandas as pd
import datetime
import traceback
from ruamel import yaml
from datetime import datetime, timedelta

s = open('./config/mysql_source.json')
source = json.load(s)
s.close()
con_source = pymysql.connect(
    host=source['HOST'],
    user=source['USER'],
    password=source['PASS'],
    database=source['DBNAME'],
    cursorclass=pymysql.cursors.DictCursor
)

t = open('./config/mysql_target.json')
target = json.load(t)
t.close()
con_target = pymysql.connect(
    host=target['HOST'],
    user=target['USER'],
    password=target['PASS'],
    database=target['DBNAME'],
    cursorclass=pymysql.cursors.DictCursor
)

# then = datetime.today() - timedelta(1)
# yesterday = then.strftime('%Y-%m-%d %H:%M:%S')
# now = datetime.now()
# today = now.strftime('%Y-%m-%d %H:%M:%S')

yesterday = '2021-07-01 00:00:00'
today = '2021-07-01 23:59:59'

def query_data_source():
    with con_source.cursor() as c_source:
        c_source.execute(f"""
            SELECT
            	t.M_User_id_user user,
            	COUNT(id_transaction_addon_detail) rows
            FROM
            	Transactions t
            JOIN Transaction_Detail td ON
            	t.id_transaction = td.Transactions_id_transaction
            JOIN Transaction_Addon_Detail tad ON
            	tad.Transaction_Detail_id_transaction_detail = td.id_transaction_detail
            JOIN Add_Ons_Detail ad ON
            	ad.id_add_ons_detail = tad.Add_Ons_Detail_id_add_ons_detail
            WHERE
            	transaction_tgl BETWEEN '{yesterday}' AND '{today}'
            AND transaction_refund = 0
            AND Transaction_purpose IN ('5','9')
            	AND status = '1'
            GROUP BY
            	t.M_User_id_user
        """)

        rows = c_source.fetchall()

    return rows

def query_data_target():
    with con_target.cursor() as c_target:
        c_target.execute(f"""
            SELECT
            	M_User_id_user user,
            	COUNT(raw_mart_sales_addon_detail_id_transaction_addon_detail) total
            FROM
            	raw_mart_sales_addon_detail
            WHERE
            	raw_mart_sales_addon_detail_datetime BETWEEN '{yesterday}' AND '{today}'
            GROUP BY
            	M_User_id_user
        """)
        rows = c_target.fetchall()

    return rows

def history_validity(data):
    cur = con_target.cursor()

    sql = f"""
        INSERT INTO validity_history (etl_name, id_user, data_target, data_source, status, actdate, ts_current, ts_end)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """
    ins = ['Sub-Varian', data[0], data[1], data[2], data[3], datetime.now(), yesterday, today]

    cur.execute(sql, ins)
    con_target.commit()
    cur.close()

def processValidity():
    ## DATAFRAME
    d_source = query_data_source()
    dict_source = {i['user']:i['rows'] for i in d_source}

    d_target = query_data_target()
    dict_target = {i['user']:i['total'] for i in d_target}

    dict_result = {}
    dict_inconsistent = {}
    for i in d_target:
        ## check data user exist
        if i['user'] in dict_source:
            if i['total'] != dict_source[i['user']]:
                dict_inconsistent[i['user']] = 1
                dict_result[i['user']] = 1

                log = [i['user'], i['total'], dict_source[i['user']], 'not valid']
                history_validity(log)
        else:
            dict_inconsistent[i['user']] = 0
            dict_result[i['user']] = 0

            log = [i['user'], i['total'], 0, 'not found']
            history_validity(log)

    print(f"Total data source {len(dict_source)}")
    print(f"Total data source {len(dict_target)}")
    print(dict_inconsistent)

    time_process = time.time() - start_time
    print("--- %s seconds ---" % (time_process))

    result = {
        'data_inconsistent': dict_inconsistent,
        'rows_source': len(dict_source),
        'rows_target': len(dict_target),
        'time_process': time_process
    }

    return result
