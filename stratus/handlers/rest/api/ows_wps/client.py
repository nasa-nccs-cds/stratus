from stratus.app.client import StratusClient, stratusrequest
from typing import Dict, Optional, List
import time, os, requests, json, pickle, xarray as xa
from stratus_endpoint.util.config import StratusLogger
from stratus_endpoint.handler.base import TaskHandle, Status, TaskResult
from stratus.app.core import StratusCore
from xml.etree.ElementTree import Element
import defusedxml.ElementTree as ET
from owslib.wps import WebProcessingService, WPSExecution, monitorExecution
from enum import Enum
MB = 1024 * 1024

class MessageState(Enum):
    ARRAY = 0
    FILE = 1
    RESULT = 2

class OwsWpsTask(TaskHandle):

    def __init__(self, rid: str, cid: str, wpsRequest: WPSExecution, **kwargs):
        TaskHandle.__init__( self, rid=rid, cid=cid, **kwargs )
        self.logger = StratusLogger.getLogger()
        self.execution: WPSExecution = wpsRequest
        self._statMessage = None
        self._status = Status.UNKNOWN
        self.cacheDir: str = self.createCache( **kwargs )

    def createCache(self, **kwargs ) -> str:
        cacheDir: str = os.path.expanduser( kwargs.get( "cache", "~/.edas/cache" ) )
        try: os.makedirs( cacheDir )
        except: pass
        return cacheDir

    def exception(self) -> Optional[Exception]:
        return self.execution.errors[0] if len(self.execution.errors) > 0 else None

    def getResult( self, **kwargs ) ->  Optional[TaskResult]:
        status = self.status()
        header = {}
        block = kwargs.get("block")
        if block: self.waitUntilReady()
        if status == Status.ERROR:
            for ex in self.execution.errors:
                header[f"Error-{ex.code}"] = ex.text
                self.logger.error('WPS Execution Error: code=%s, locator=%s, text=%s' % (ex.code, ex.locator, ex.text))
            return TaskResult(header)
        elif status == Status.EXECUTING:
            return None
        elif status == Status.COMPLETED:
            type = kwargs.get("type", "file")
            for output in self.execution.processOutputs:
                output_content = output.retrieveData( self.execution.username, self.execution.password, headers=self.execution.headers, verify=self.execution.verify, cert=self.execution.cert)
                header.update( self.execution.headers )
                header["Reference"] = output.reference
                header["FileName"] = output.fileName
                if type == "file":
                    filepath = f"{self.cacheDir}/{output.fileName}"
                    out = open(filepath, 'wb')
                    if output_content is b'' and len(output.data) > 0:
                        for data in output.data:
                            output_content = output_content + data
                    out.write(output_content)
                    out.close()
                    self.logger.info('Output written to file: %s' % filepath)
                else:
                    if output_content is b'' and len(output.data) > 0:
                        results = [pickle.loads( data, encoding="bytes" ) for data in output.data ]
                        return TaskResult( header, results )
                    else:
                        results = [ pickle.loads( output_content, encoding="bytes" ) ]
                        return TaskResult( header, results )
        return None


    def status(self) ->  Status:
        self.execution.checkStatus()
        if self.execution.isComplete():
            if self.execution.isSucceded():
                return Status.COMPLETED
            else:
                return Status.ERROR
        else:
            return Status.EXECUTING

class OwsWpsClient(StratusClient):

    def __init__( self, **kwargs ):
        super(OwsWpsClient, self).__init__( "rest", **kwargs )
        if "host_address" in self.parms:
            self.host_address = self["host_address"]
        else:
            host = self["host"]
            port = self["port"]
            route = self.parm("route","ows_wps")
            self.host_address = f"http://{host}:{port}/{route}"
        self.wpsRequest: WebProcessingService = WebProcessingService( self.host_address, verbose=False, skip_caps=True )
        self.wpsRequest.getcapabilities()

    def getCapabilitiesStr( self, type ): return '%s?request=getCapabilities&service=WPS&identifier=%s' % ( self.host_address, type )

    @stratusrequest
    def request( self, requestSpec: Dict, inputs: List[TaskResult] = None, **kwargs ) -> TaskHandle:
        inputs = [ ( key, json.dumps(value) ) for key,value in requestSpec.items() ]
        response: WPSExecution =  self.wpsRequest.execute( "WORKFLOW", inputs )
        self.logger.info( "Got response xml: " + str(response["xml"]) )
        self.logger.info("Got refs: " + str(response["refs"]))
        return OwsWpsTask( requestSpec['rid'], self.cid, response, cache=self.cache_dir )

    def execJsonRequest( self, requestURL, parms: Dict) -> Dict:
        response: requests.Response = requests.get(requestURL, params=parms)
        self.logger.info( f"SUBMIT JSON Request {requestURL} with parms: {parms}\n  Response: \n {response.text}" )
        if response.ok:
            return json.loads(response.text)
        else:
            raise Exception(response.text)

    def execRequest(self, requestURL, parms: Dict) -> Element:
        response: requests.Response = requests.get(requestURL, params=parms)
        return ET.fromstring(response.text)

    def capabilities(self, type: str, **kwargs ) -> Dict:
        if type == "epas":
            request_parms = dict( request='getCapabilities', service='WPS', identifier='epas' )
            return self.execJsonRequest( self.host_address, request_parms )
        return { op.name:str(op) for op in self.wpsRequest.operations }

    def log(self, msg: str ):
        self.logger.info( "[RP] " + msg )

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        if self.active:
            self.active = False


if __name__ == "__main__":
    from stratus.util.test import TestDataManager as mgr
    HERE = os.path.dirname(os.path.abspath(__file__))
    SETTINGS_FILE = os.path.join(HERE, "client_test_settings.ini")

    core = StratusCore( SETTINGS_FILE )
    client = core.getClient()

    local_request = dict(
        domain=[{"name": "d0", "lat": {"start": 50, "end": 55, "system": "values"},
                 "lon": {"start": 40, "end": 42, "system": "values"},
                 "time": {"start": "1980-01-01", "end": "1981-12-31", "crs": "timestamps"}}],
        input=[{"uri": mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}],
        operation=[ { 'name': "xarray.ave", 'axes': "t", "input": "v0"} ]
    )

    edas_server_request = dict(
        domain=[{"name": "d0", "lat": {"start": 50, "end": 55, "system": "values"},
                 "lon": {"start": 40, "end": 42, "system": "values"},
                 "time": {"start": "1980-01-01", "end": "1981-12-31", "crs": "timestamps"}}],
        input=[{"uri": "collection://cip_cfsr_mth", "name": "tas:v0", "domain": "d0"}],
        operation=[ { 'name': "xarray.ave", 'axes': "t", "input": "v0"} ]
    )

    task: OwsWpsTask = client.request( edas_server_request )
    print( task.status() )
    result = task.getResult( block=True )
    print( "Got Result: " + str(result.header) )

