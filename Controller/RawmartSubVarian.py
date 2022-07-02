import falcon
import json
from datetime import datetime
from Models import ValiditySubVarian

class validity(object):
    def on_post(self, req, resp):
        resp.status = falcon.HTTP_200
        data = {'status': 'unavailable service'}
        resp.body = json.dumps(data)

    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        content=ValiditySubVarian.processValidity()
        data = {'status':'success','content':content,'enum':'1'}
        resp.body = json.dumps(data)
