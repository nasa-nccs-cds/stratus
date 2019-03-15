from flask import request, Blueprint, make_response
from stratus_endpoint.handler.base import Task, TaskResult
from stratus.handlers.client import StratusClient
import pickle, ctypes, json, requests, flask
from jinja2 import Environment, PackageLoader, select_autoescape
from stratus.handlers.app import StratusAppBase
from typing import *
from stratus.handlers.rest.app import RestAPIBase

class RestAPI(RestAPIBase):
    debug = False

    def __init__( self, name: str, app: StratusAppBase, **kwargs ):
        RestAPIBase.__init__( self, name, app, **kwargs )
        self.jenv = Environment( loader=PackageLoader('stratus', 'templates'), autoescape=select_autoescape(['html','xml']) )
        self.templates = {}
        self.templates['execute_response'] = self.jenv.get_template('execute_response.xml')
        self.dapRoute = kwargs.get( "dapRoute", None )

    def parseDatainputs(self, datainputs: str) -> Dict:
        raw_datainputs = ctypes.create_string_buffer(datainputs.strip())
        if raw_datainputs[0] == "[":
            assert raw_datainputs[-1] == "]", "Datainouts format error: missing external brackets: " + raw_datainputs
            raw_datainputs[0] = "{"
            raw_datainputs[-1] = "}"
        return json.loads(raw_datainputs)

    def processRequest( self, requestDict: Dict ) -> flask.Response:
        if self.debug: self.logger.info(f"Processing Request: '{str(requestDict)}'")
        current_tasks = self.app.processWorkflow(requestDict)
        if self.debug: self.logger.info("Current tasks: {} ".format(str(list(current_tasks.items()))))
        for task in current_tasks.values(): self.addTask( task )
        return self.executeResponse( requestDict )

    def executeResponse(self, response: Dict ) -> flask.Response:
        status = response["status"]
        rid = response["rid"]
        route = request.path
        if status == "executing":
            status = dict( tag="ProcessStarted", mesage=response["message"] )
            url = dict( status=f"{route}/status?id={rid}", file=f"{route}/file?id={rid}" )
            if self.dapRoute is not None: url['dap'] = f"{self.dapRoute}/{rid}.nc"
            responseXml = self.templates['execute_response'].render( dict( status=status, url=url) )
            return flask.Response( response=responseXml, status=202, mimetype="application/xml" )
        elif status == "error":
            status = dict( tag="ProcessFailed", mesage=response["message"] )
            url = dict( status=f"{route}/status?id={rid}")
            responseXml = self.templates['execute_response'].render( dict( status=status, url=url) )
            return flask.Response( response=responseXml, status=400, mimetype="application/xml" )


    def _addRoutes(self, bp: Blueprint):

        @bp.route('/wps', methods=('GET'), endpoint='exe')
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
            rid = self.getParameter( "rid", None, False)
            statusMap = self.getStatus(rid)
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



