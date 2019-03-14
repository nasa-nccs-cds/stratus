from flask import request, Blueprint, make_response
from stratus_endpoint.handler.base import Task, TaskResult
from stratus.handlers.client import StratusClient
import pickle, ctypes, json, requests
from typing import *
from stratus.handlers.rest.app import RestAPIBase

class RestAPI(RestAPIBase):
    debug = False

    def parseDatainputs(self, datainputs: str) -> Dict:
        raw_datainputs = ctypes.create_string_buffer(datainputs.strip())
        if raw_datainputs[0] == "[":
            assert raw_datainputs[-1] == "]", "Datainouts format error: missing external brackets: " + raw_datainputs
            raw_datainputs[0] = "{"
            raw_datainputs[-1] = "}"
        return json.loads(raw_datainputs)

    def processRequest( self, requestDict: Dict ) -> requests.Response:
        if self.debug: self.logger.info(f"Processing Request: '{str(requestDict)}'")
        current_tasks = self.app.processWorkflow(requestDict)
        if self.debug: self.logger.info("Current tasks: {} ".format(str(list(current_tasks.items()))))
        for task in current_tasks.values(): self.addTask( task )
        return self.jsonResponse( dict( status="executing", rid=requestDict['rid'] ), code=202 )

    def _addRoutes(self, bp: Blueprint):

        @bp.route('/wps', methods=('GET'))
        def exe():
            requestArg = request.args.get("request", None)
            if requestArg == "Execute":
                datainputs = request.args.get("datainputs", None)
                inputsArg = self.parseDatainputs( datainputs )
                return self.processRequest( inputsArg )
            elif requestArg == "GetCapabilities":
                id = request.args.get("id",  None )
                return self.getCapabilities(id)
            elif requestArg == "DescribeProcess":
                id = request.args.get("id",  None )
                return self.describeProcess(id)
            else:
                return self.errorResponse( 400, "Illegal request type: " + requestArg )

        @bp.route('/status', methods=('GET',))
        def status():
            cid = self.getParameter( "cid", None, False)
            statusMap = self.getStatus(cid)
            if self.debug: self.logger.info( "Status Map: " + str(statusMap) )
            return self.jsonResponse( statusMap )

        @bp.route('/file', methods=('GET',))
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



