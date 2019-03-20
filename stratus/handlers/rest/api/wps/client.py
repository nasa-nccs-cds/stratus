from stratus.handlers.client import StratusClient, stratusrequest
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
import traceback, time, logging, xml, json, requests
from stratus.util.config import Config, StratusLogger, UID
from stratus.util.parsing import s2b, b2s
from stratus_endpoint.handler.base import Task, Status, TaskResult
from stratus.handlers.core import StratusCore
import random, string, os, pickle, queue
from stratus.handlers.rest.api.wps.wpsRequest import WPSExecuteRequest
import xarray as xa
from enum import Enum
MB = 1024 * 1024

class MessageState(Enum):
    ARRAY = 0
    FILE = 1
    RESULT = 2

class WPSRestClient(StratusClient):

    def __init__( self, **kwargs ):
        super(WPSRestClient, self).__init__( "rest", **kwargs )
        if "host_address" in self.parms:
            self.host_address = self["host_address"]
        else:
            host = self["host"]
            port = self["port"]
            route = self["route"]
            self.host_address = f"http://{host}:{port}/{route}"
        self.wpsRequest = WPSExecuteRequest(self.host_address)

    @stratusrequest
    def request( self, requestSpec: Dict, **kwargs ) -> Task:
        response =  self.wpsRequest.exe(requestSpec)
        self.log( "Got response xml: " + str(response["xml"]) )
        self.log("Got refs: " + str(response["refs"]))
        return RestTask( requestSpec['rid'], self.cid, response["refs"], self.wpsRequest )

    def capabilities(self, type: str, **kwargs ) -> Dict:
        return self.wpsRequest.getCapabilities( type )

    def log(self, msg: str ):
        self.logger.info( "[RP] " + msg )

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        if self.active:
            self.active = False

class RestTask(Task):

    def __init__(self, rid: str, cid: str, refs: Dict, wpsRequest: WPSExecuteRequest, **kwargs):
        super(RestTask, self).__init__( rid, cid, **kwargs )
        self.logger = StratusLogger.getLogger()
        self.statusUrl: str  = refs.get("status",None)
        self.fileUrl: str = refs.get("file", None)
        self.dataUrl: str = refs.get("data", None)
        self.dapUrl: str = refs.get("dap", None)
        self.wpsRequest: WPSExecuteRequest = wpsRequest
        self._statMessage = None
        self._status = Status.UNKNOWN
        self.cacheDir: str = self.createCache( **kwargs )

    def createCache(self, **kwargs ) -> str:
        cacheDir: str = kwargs.get( "cache", os.path.expanduser("~/.edas/cache") )
        try: os.makedirs( cacheDir )
        except: pass
        return cacheDir

    def getResult( self, **kwargs ) ->  Optional[TaskResult]:
        timeout = kwargs.get("timeout")
        block = kwargs.get("block")
        raiseErrors = kwargs.get("raiseErrors")
        type = kwargs.get("type","file")
        self.status()
        self.logger.info( "*STATUS: " +  str(self._status) )
        while self._status == Status.IDLE or self._status == Status.EXECUTING:
            time.sleep(1)
            self.status()
            self.logger.info( "*STATUS: "  +  str(self._status) )
        if self._status == Status.ERROR:
            self.logger( " *** Remote execution error: " + self._statMessage )
            if raiseErrors: raise Exception( self._statMessage )
            return None
        elif self._status == Status.COMPLETED:
            if type == "file":
                filePath = self.cacheDir + "/" + self.fileUrl.split('=')[-1] + ".nc"
                self.wpsRequest.downloadFile( filePath, self.fileUrl )
                return TaskResult( dict( file=filePath) )
            else:
                xarray = self.wpsRequest.downloadData(self.dataUrl)
                return TaskResult( { **self._parms, "rid": self.rid, "cid": self.cid }, [xarray] )

    def status(self) ->  Status:
        stat = self.wpsRequest.getStatus(self.statusUrl)
        statStr = stat["status"]
        if statStr == "ProcessStarted": self._status = Status.EXECUTING
        elif statStr == "ProcessFinished": self._status = Status.COMPLETED
        elif statStr == "ProcessAccepted": self._status = Status.IDLE
        elif statStr == "ProcessFailed": self._status = Status.ERROR
        elif statStr == "ProcessSucceeded": self._status = Status.COMPLETED
        elif statStr == "ProcessUnknown": self._status = Status.UNKNOWN
        self._statMessage = stat["message"]
        return self._status

    @property
    def statusMessage(self):
        return self._statMessage

    def __str__(self) -> str:
        items = dict( rid=self._rid, cid=self._cid, status=self._status, statusUrl=self.statusUrl, fileUrl=self.fileUrl, dapUrl=self.dapUrl )
        return f"{self.__class__.__name__}{str(items)}"

if __name__ == "__main__":
    from stratus.util.test import TestDataManager as mgr
    HERE = os.path.dirname(os.path.abspath(__file__))
    SETTINGS_FILE = os.path.join(HERE, "client_test_settings.ini")

    core = StratusCore( SETTINGS_FILE )
    client = core.getClient()
    client.init()

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

    task: RestTask = client.request( edas_server_request )
    print( task.status() )
    print( task.statusMessage )
    result = task.getResult()
    print( "Got Result: " + str(result.header) )

