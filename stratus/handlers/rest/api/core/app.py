from flask import request, Blueprint, make_response
from stratus_endpoint.handler.base import TaskHandle, TaskResult
import pickle
from typing import *
from stratus.handlers.rest.app import RestAPIBase

class RestAPI(RestAPIBase):
    debug = True

    def _addRoutes(self, bp: Blueprint):

        @bp.route('/exe', methods=('GET', 'POST'))
        def exe():
            if request.method == 'POST':    requestDict: Dict = request.json
            else:                           requestDict: Dict = self.jsonRequest( request.args.get("request",None) )
            if self.debug: self.logger.info(f"Processing rest-core Request: '{str(requestDict)}'")
            requestDict = self.app.submitWorkflow(requestDict)
            return self.jsonResponse( dict( status="executing", rid=requestDict['rid'] ), code=202 )

        @bp.route('/status', methods=('GET',))
        def status():
            rid = self.getParameter( "rid", None, False)
            statusMap = self.getStatus(rid)
            if self.debug: self.logger.info( "Status Map: " + str(statusMap) )
            return self.jsonResponse( statusMap )

        @bp.route('/result', methods=('GET',))
        def result():
            rid = self.getParameter("rid")
            task:  Optional[TaskHandle] = self.app.getResult( rid )
            result: Optional[TaskResult] = task.getResult() if task is not None else None
            if result is None:
                return self.jsonResponse( dict(status="executing", id=task.rid) )
            else:
                response = make_response( pickle.dumps( result ) )
                response.headers.set('Content-Type', 'application/octet-stream')
                response.headers.set('Content-Format', 'xarray-dataset' )
                self.app.clearWorkflow( rid )
                return response

        @bp.route('/capabilities', methods=('GET',))
        def capabilities():
            ctype = self.getParameter("type",self.getParameter("identifier","epas"))
            self.logger.info( "REST_APP: Processing capabilities request, type = " + str(ctype) + ", parms = " + str(request.args))
            response: Dict = self.app.core.getCapabilities( ctype )
            self.logger.info( f"REST_APP: Sending capabilities response: {response}" )
            return self.jsonResponse( response )




