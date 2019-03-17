from flask import request, Blueprint, make_response
from stratus_endpoint.handler.base import Task, TaskResult
from stratus.handlers.client import StratusClient
import pickle
from typing import *
from stratus.handlers.rest.app import RestAPIBase

class RestAPI(RestAPIBase):
    debug = False

    def _addRoutes(self, bp: Blueprint):

        @bp.route('/exe', methods=('GET', 'POST'))
        def exe():
            if request.method == 'POST':    requestDict: Dict = request.json
            else:                           requestDict: Dict = self.jsonRequest( request.args.get("request",None) )
            if self.debug: self.logger.info(f"Processing Request: '{str(requestDict)}'")
            current_tasks = self.app.processWorkflow(requestDict)
            if self.debug: self.logger.info("Current tasks: {} ".format(str(list(current_tasks.items()))))
            for task in current_tasks.values(): self.addTask( task )
            return self.jsonResponse( dict( status="executing", rid=requestDict['rid'] ), code=202 )

        @bp.route('/status', methods=('GET',))
        def status():
            cid = self.getParameter( "cid", None, False)
            statusMap = self.getStatus(cid)
            if self.debug: self.logger.info( "Status Map: " + str(statusMap) )
            return self.jsonResponse( statusMap )

        @bp.route('/result', methods=('GET',))
        def result():
            rid = self.getParameter("rid")
            task: Task = self.tasks.get( rid, None )
            assert task is not None, f"Can't find task for rid {rid}, current tasks: {str(list(self.tasks.keys()))}"
            result: Optional[TaskResult] = task.getResult()
            if result is None:
                return self.jsonResponse( dict(status="executing", id=task.rid) )
            else:
                response = make_response( pickle.dumps( result ) )
                response.headers.set('Content-Type', 'application/octet-stream')
                response.headers.set('Content-Class', 'xarray-dataset' )
                self.removeTask( rid )
                return response

        @bp.route('/capabilities', methods=('GET',))
        def capabilities():
            ctype = self.getParameter("identifier","")
            self.logger.info( "REST_APP: Processing capabilities request, type = " + str(ctype) + ", parms = " + str(request.args))
            response: Dict = self.app.core.getCapabilities( ctype )
            return self.jsonResponse( response )




