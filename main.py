import falcon
from falcon_multipart.middleware import MultipartMiddleware
from falcon_cors import CORS
from waitress import serve
from logger import Logger
from datetime import datetime
import pytz
import json
from Middleware.LoggingMiddleware import LoggingMiddleware
from Controller import RawmartAkunting

# LOGGER = Logger()

cors = CORS(allow_all_origins=True, allow_all_headers=True, allow_all_methods=True)

api = falcon.API(middleware=[ cors.middleware, MultipartMiddleware(), LoggingMiddleware() ])

# def generic_error_handler(ex, req, resp, params):
#     LOGGER.log_access("error", ex,  req, json.dumps({"title":"500 Internal Server Error"}),  res_time = datetime.now(pytz.timezone("Asia/Jakarta")), traceback="\n".join(traceback.format_exc()), stack = 2)
#     raise falcon.HTTPServiceUnavailable(title="500 Internal Server Error") #exit
#
# api.add_error_handler(Exception, generic_error_handler)

api.add_route('/validity/rawmart/akunting', RawmartAkunting.validity())

serve(api, host='0.0.0.0', port=8001, threads=1000)

# import great_expectations as ge
# from great_expectations.checkpoint import SimpleCheckpoint
# from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource, check_if_datasource_name_exists
# from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
# from great_expectations.exceptions import DataContextError
# context = ge.get_context()
#
# table_name = "Akunting"
#
# # Target
# host = "10.130.1.107"
# port = "3306"
# username = "dev_etl"
# password = "m4jOO@123mart!"
# database = "mart"
# schema_name = "mart"
#
# # Source
# shost = "10.130.248.105"
# sport = "3306"
# susername = "akunting_dev"
# spassword = "m4jOOAkunt1N99#2021"
# sdatabase = "prd_akunting"
# sschema_name = "prd_akunting"
#
# import pymysql.cursors
# con = pymysql.connect(
#     host=shost,
#     user=susername,
#     password=spassword,
#     database=sdatabase,
#     cursorclass=pymysql.cursors.DictCursor)
#
# with con.cursor() as dml1:
#     dml1.execute("""
#     SELECT
#     count(*) as total_data
#     FROM
#     """ + table_name)
#     rows1 = dml1.fetchall()
#
# print(rows1)
