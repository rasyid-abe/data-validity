import falcon
from falcon_multipart.middleware import MultipartMiddleware
from falcon_cors import CORS
from waitress import serve
from logger import Logger
from datetime import datetime
import pytz
import json
from Middleware.LoggingMiddleware import LoggingMiddleware
from Controller import RawmartVarian, RawmartSubVarian, RawmartProduk

LOGGER = Logger()
cors = CORS(allow_all_origins=True, allow_all_headers=True, allow_all_methods=True)
api = falcon.API(middleware=[ cors.middleware, MultipartMiddleware(), LoggingMiddleware() ])

def generic_error_handler(ex, req, resp, params):
    LOGGER.log_access("error", ex,  req, json.dumps({"title":"500 Internal Server Error"}),  res_time = datetime.now(pytz.timezone("Asia/Jakarta")), traceback="\n".join(traceback.format_exc()), stack = 2)
    raise falcon.HTTPServiceUnavailable(title="500 Internal Server Error") #exit

api.add_error_handler(Exception, generic_error_handler)
api.add_route('/validity/rawmart/varian', RawmartVarian.validity())
api.add_route('/validity/rawmart/sub_varian', RawmartSubVarian.validity())
api.add_route('/validity/rawmart/produk', RawmartProduk.validity())

serve(api, host='0.0.0.0', port=8101, threads=1000)
