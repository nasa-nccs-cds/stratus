from flask import request, Blueprint, make_response
from stratus_endpoint.handler.base import TaskResult, Status
import xarray as xa
from stratus_endpoint.util.config import UID
import pickle, json, flask, os
from jinja2 import Environment, PackageLoader, select_autoescape
from stratus_endpoint.handler.base import TaskHandle
from stratus.app.base import StratusAppBase
from typing import *
from stratus.handlers.rest.app import RestAPIBase
HERE = os.path.dirname(os.path.abspath(__file__))
TEMPLATES = os.path.join(HERE, "templates")

class RestAPI(RestAPIBase):
    debug = True
    API = "ows_wps"

    def __init__(self, name: str, app: StratusAppBase, **kwargs):
        RestAPIBase.__init__( self, name, app, **kwargs )
        self.jenv = Environment( loader=PackageLoader( f'stratus.handlers.rest.api.{self.API}',  "templates" ), autoescape=select_autoescape(['html','xml']) )
        self.templates = { template:self.jenv.get_template(f'{template}.xml') for template in ["describe_process", "execute_response", "get_capabilities"] }
        self.dapRoute = kwargs.get( "dapRoute", None )
        self.secureConnection = False

    def parseDatainputs(self, datainputs: str) -> Dict:
        if datainputs is None: return {}
        raw_datainputs = datainputs.strip()
        if raw_datainputs[0] == "[": raw_datainputs = raw_datainputs[1:-1]
        json_datainputs = "{"+raw_datainputs.replace("domain=",'"domain":').replace("variable=",'"variable":').replace("operation=",'"operation":').replace(";",',')+"}"
        self.logger.info( "json datainputs = " + json_datainputs )
        return json.loads(json_datainputs)

    def processRequest( self, requestDict: Dict ) -> flask.Response:
        rid = requestDict.setdefault( "rid", UID.randomId(6) )
        if self.debug: self.logger.info(f"Processing ows-wps Request: '{str(requestDict)}'")
        try:
            self.app.submitWorkflow(requestDict)
            return self.executeResponse( dict( status="executing", message="Executing Request", rid=rid ) )
        except Exception as err:
            return self.executeResponse(dict(status="error", message=getattr(err, 'message', repr(err)), rid=rid))

    def executeResponse(self, response: Dict[str,str] ) -> flask.Response:
        if self.debug: self.logger.info( " #####>>>> response: " + str(response) )
        rid = response.get("rid","XXXXXX")
        message = response.get("message", "")
        status = response["status"]
        if status == "executing":
            responseXml = self._getStatusXml("ProcessStarted", message, rid )
        elif status == "completed":
            responseXml = self._getStatusXml("ProcessSucceeded", message, rid )
        elif status == "idle":
            responseXml = self._getStatusXml("ProcessAccepted", message, rid )
        elif status == "error":
            responseXml = self._getStatusXml("ProcessFailed", "Error: " + message, rid, False )
        elif status == "canceled":
            responseXml = self._getStatusXml("ProcessFailed", "Process Cancelled: " + message, rid, False)
        elif status == "unknown":
            responseXml = self._getStatusXml("ProcessPaused", "Unregistered Process: " + rid , rid, False )
        else: raise Exception( "Unknown status: " + status )
        return flask.Response( response=responseXml, status=200, mimetype="application/xml" )

    def getErrorResponse(self, message, code=400 ) -> flask.Response:
        json_content = json.dumps( dict(status="error", message=message) )
        return flask.Response(response=json_content, status=code, mimetype="application/json")

    def _getStatusXml(self, status: str, message: str, rid: str, addDataRefs = True) -> str :
        status = dict( tag=status, message=message )
        url = dict( status=f"{request.url_root}{self.API}/status?rid={rid}" )
        process = dict( identifier="workflow", title="", abstract="", profile="" )
        if addDataRefs:
            url['file'] = f"{request.url_root}{self.API}/file?rid={rid}"
            if self.secureConnection:
                url['data'] = f"{request.url_root}{self.API}/data?rid={rid}"
            if self.dapRoute is not None:
                url['dap'] = f"{self.dapRoute}/{rid}.nc"
        return self.render( 'execute_response', status=status, url=url, process=process )

    def _getCapabilitiesXml(self, capabilitiesData: Dict )-> str:
        manager = dict(name="Thomas Maxwell", position="EDAS Developer", email="thomas.maxwell@nasa.gov")
        server = dict( title="EDAS", description="Earth Data Analytic Services", manager=manager)
        processes = []
        server["processes"] = processes
        modules = capabilitiesData.get("modules",[])
        for module in modules:
            for  kernel in module.kernels:
                processes.append( kernel )
        print( "**SERVER = " + str(server) )
        return self.render('get_capabilities', server=server )

    def render(self, template: str, **kwargs ):
        assert template in self.templates, f"Unknown template {template}, existing templates: {str(self.templates.keys())}"
        return self.templates[template].render( kwargs )

    def getCapabilities(self, ctype: str ) -> flask.Response:
        response: Dict = self.app.core.getCapabilities(ctype)
        self.logger.info( f"getCapabilities[{ctype}]: response: {response}")
        if ctype == "epas":
            responseJson = json.dumps( response )
            return flask.Response(response=responseJson, status=200, mimetype="application/json" )
        else:
            responseXml = self._getCapabilitiesXml( response )
            return flask.Response(response=responseXml, status=200, mimetype="application/xml" )

    def describeProcess(self, ctype: str ) -> flask.Response:
        responseXml = ""
        return flask.Response(response=responseXml, status=200, mimetype="application/xml" )

    def missingResult(self, task) -> flask.Response:
        if task.status() == Status.CANCELED: return self.getErrorResponse("Task was canceled")
        elif task.status() == Status.ERROR:  return self.getErrorResponse(f"Task threw an exception: {repr(task.exception())}")
        else:                                return self.getErrorResponse(f"Task workflow failed to return any results")

    def _addRoutes(self, bp: Blueprint):
        self.logger.info( "Adding WPS routes" )
        @bp.route('/cwt', methods=['GET'] )
        def exe():
            raw_request_data = request.args
            requestData = { key.lower(): value for key,value in raw_request_data.items() }
            self.logger.info( f" *** REQUEST DATA: { {k:v for k,v in requestData.items()} }" )
            requestArg = requestData.get("request", None).lower()
            identifier = requestData.get("identifier", None)
            if self.debug: self.logger.info( "EXE: requestArg = " + requestArg + ", identifier = " + str(identifier))
            if requestArg == "execute":
                datainputs = requestData.get("datainputs", None)
                inputsArg = self.parseDatainputs( datainputs )
                return self.processRequest( inputsArg )
            elif requestArg == "getcapabilities":
                return self.getCapabilities(identifier)
            elif requestArg == "describeprocess":
                return self.describeProcess(identifier)
            else:
                return self.executeResponse( dict( status='error', message ="Illegal request type: " + requestArg ) )

        @bp.route('/status', methods=['GET'])
        def status():
            rid = self.getParameter( "rid", None, False)
            status_results = self.getStatus(rid)
            if self.debug: self.logger.info( f" ----> Status Request[{rid}]: " + str(status_results) )
            return self.executeResponse( status_results )

        @bp.route('/file', methods=['GET'])
        def file_result():
            rid = self.getParameter("rid")
            workflow = self.app.getWorkflow(rid)
            if workflow.status() == Status.EXECUTING:
                return self.jsonResponse( dict(status="executing", id=rid) )
            else:
                task: Optional[TaskHandle] = workflow.getResult()
                result: Optional[TaskResult] = task.getResult() if task is not None else None
                self.logger.info(f"Got File Request for task rid={rid}, result = {str(result)}")
                if result is None: return self.missingResult( task )
                dataset: Optional[xa.Dataset] = result.popDataset()
                if dataset is None:
                    if result.getResultClass() == "METADATA":
                        metadata = json.dumps(result.header)
                        self.logger.info(f"Sending metadata response: " + metadata )
                        response =  flask.Response( response=metadata, status=200, mimetype="application/json" )
                        response.headers.set('Content-Format', 'metrics' )
                        response.headers.set('Results-Remaining', '0')
                        return response
                    else:
                        return self.getErrorResponse( "No more results available")
                path = f"/tmp/{rid}.nc"
                self.logger.info(f"Saving temp file to {path}")
                dataset.to_netcdf( path, mode="w", format='NETCDF4' )
                with open(path, mode='rb') as ncfile:
                    response = make_response( ncfile.read() )
                    response.headers.set('Content-Type', 'application/octet-stream')
                    response.headers.set('Content-Format', 'netcdf-file' )
                    response.headers.set('Results-Remaining', str(result.size()) )
                    if result.empty(): self.app.clearWorkflow( rid )
                self.logger.info(f"Returning file response.")
                return response

        @bp.route('/data', methods=['GET'])
        def data_result():
            if not self.secureConnection:
                return self.getErrorResponse("Can't serialize data over an insecure connection")
            rid = self.getParameter("rid")
            workflow = self.app.getWorkflow(rid)
            if workflow.status() == Status.EXECUTING:
                return self.jsonResponse( dict(status="executing", id=rid) )
            else:
                task: Optional[TaskHandle] = workflow.getResult()
                result: Optional[TaskResult] = task.getResult() if task is not None else None
                self.logger.info(f"Got File Request for task rid={rid}, result = {str(result)}")
                if result is None:
                    return self.missingResult( task )
                dataset: Optional[xa.Dataset] = result.popDataset()
                if dataset is None:
                    if result.getResultClass() == "METADATA":
                        return flask.Response(response=json.dumps(result.header), status=200, mimetype="application/json")
                    else:
                        return self.getErrorResponse( "No more results available")
                self.logger.info( "Downloading pickled xa.Dataset, attrs: " + str(dataset.attrs) )
                response = make_response( pickle.dumps( dataset ) )
                response.headers.set('Content-Type', 'application/octet-stream')
                response.headers.set('Content-Format', 'xarray-dataset' )
                response.headers.set('Results-Remaining', str(result.size()))
                if result.empty(): self.app.clearWorkflow( rid )
                return response


