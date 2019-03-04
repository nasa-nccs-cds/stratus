from stratus.handlers.client import StratusClient
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
import traceback, time, logging, xml, json, requests
from stratus.util.config import Config, StratusLogger, UID
from threading import Thread
from stratus.util.parsing import s2b, b2s
from stratus_endpoint.handler.base import Task, Status, TaskResult
from stratus.handlers.app import StratusCore
import random, string, os, pickle, queue
import xarray as xa
from enum import Enum
MB = 1024 * 1024

class MessageState(Enum):
    ARRAY = 0
    FILE = 1
    RESULT = 2

class RestClient(StratusClient):

    def __init__( self, **kwargs ):
        super(RestClient, self).__init__( "rest", **kwargs )
        self.host = self["host"]
        self.port = self["port"]
        self.api = self["api"]
        self.response_manager = ResponseManager.getManger( f"http://{self.host}:{self.port}/{self.api}" )
        self.response_manager.start()

    def request( self, requestSpec: Dict, **kwargs ) -> Task:
        requestDict = dict(requestSpec)
        requestDict["sid"] = self.sid( self.parm("id",None) )
        response = self.response_manager.postMessage( "exe", requestDict, **kwargs )
        self.log( "Got response: " + str(response) )
        return restTask( requestDict['sid'], self.response_manager )

    def status(self, **kwargs ) -> Status:
        return self.response_manager.getStatus( self.sid() )

    def capabilities(self, type: str, **kwargs ) -> Dict:
        return self.response_manager.getMessage( type, {}, **kwargs )

    def log(self, msg: str ):
        self.logger.info( "[P] " + msg )

    def __del__(self):
        self.shutdown()

    def createResponseManager(self) -> "ResponseManager":
        return self.response_manager

    def shutdown(self):
        if self.active:
            self.active = False
            if not (self.response_manager is None):
                self.response_manager.term()
                self.response_manager = None

    def waitUntilDone(self):
        self.response_manager.join()

class ResponseManager(Thread):

    managers: Dict[str,"ResponseManager"] = {}

    def __init__(self, host_address: str, **kwargs ):
        Thread.__init__(self)
        self.logger = StratusLogger.getLogger()
        self.host_address = host_address
        self.active = True
        self.setName('STRATUS zeromq client Response Thread')
        self.setDaemon(True)
        self.poll_freq = kwargs.get( "poll_freq", 0.5 )
        self.timeout = kwargs.get("timeout", 60.0)
        self.statusMap: Dict[str,Status] = {}

    @classmethod
    def getManger( cls, host_address: str)  ->  "ResponseManager":
        return cls.managers.setdefault( host_address, ResponseManager(host_address) )

    def run(self):
        debug = False
        try:
            self.log("Run RM thread")
            while( self.active ):
                statMap = self._getStatusMap()
                if debug: self.logger.info( "Server Job Status: " + str( statMap ) )
                self.statusMap.update( statMap )
                time.sleep( self.poll_freq )

        except Exception as err:
            self.log( "ResponseManager error: " + str(err) )
            self.log( traceback.format_exc() )
            self.statusMap.clear()

    def getMessage(self, type: str, requestSpec: Dict, **kwargs ) -> Dict:
        debug = False
        request_params = dict(requestSpec)
        request_params.update(kwargs)
        address = f"{self.host_address}/{type}"
        if debug: self.log(f"REQUEST[{address}](status): {str(request_params)}")
        response: requests.Response = requests.get( address, params=request_params )
        if debug: self.log( f"RESPONSE[{response.encoding}]({response.url}): {str(response)}: {response.text}"  )
        return self.unpackResponse( response )

    def postMessage(self, type: str, requestSpec: Dict, **kwargs ) -> Dict:
        request_params = dict(requestSpec)
        request_params.update(kwargs)
        self.logger.info( f"POSTing request: {str(requestSpec)}")
        response: requests.Response = requests.post( f"{self.host_address}/{type}", json=requestSpec )
        self.log( f"RESPONSE[{response.encoding}]({response.url}): {str(response)}: {response.text}"  )
        return self.unpackResponse( response )

    def unpackResponse(self, response: requests.Response )-> Dict:
        if( response.ok ):
            if response.encoding == "application/octet-stream":  response = { "content": pickle.loads( response.content ) }
            else:                                                response = response.json()
        else:                                                    response = {"error": response.status_code, "message": response.text}
        return response

    def getResult(self, sid: str, block=True, timeout=None  ) ->  Optional[TaskResult]:
        if block: self.waitUntilReady( sid, timeout )
        result = self.getMessage( "result", dict(sid=sid) )
        return result["content"]

    def _getStatusMap(self) -> Dict:
        return self.getMessage( "status", {}, timeout=self.timeout  )

    def getStatus( self, sid ) -> Status:
        return self.statusMap.get( sid, Status.UNKNOWN )

    def completed(self, sid ) -> bool :
        return self.getStatus(sid) in [Status.COMPLETED, Status.ERROR ]

    def waitUntilReady( self, sid: str, timeout: float = None ):
        accum_time = 0.0
        while not self.completed( sid ):
            time.sleep(0.2)
            if timeout is not None:
                accum_time += 0.2
                if( accum_time >= timeout ):
                    return False
        return True

    def log(self, msg: str ):
        self.logger.info( "[RM] " + msg )

    def term(self):
        self.active = False

class restTask(Task):

    def __init__(self, sid: str, manager: ResponseManager, **kwargs):
        super(restTask, self).__init__( sid, **kwargs )
        self.logger = StratusLogger.getLogger()
        self.manager: ResponseManager = manager

    def getResult(self, block=True, timeout=None ) ->  Optional[TaskResult]:
        return self.manager.getResult( self.sid, block,timeout )

    def status(self) ->  Status:
        return self.manager.getStatus( self.sid )


if __name__ == "__main__":
    from stratus.util.test import TestDataManager as mgr
    HERE = os.path.dirname(os.path.abspath(__file__))
    SETTINGS_FILE = os.path.join(HERE, "client_test_settings.ini")

    core = StratusCore( settings=SETTINGS_FILE )
    client = core.getClient()
    client.init()
    request = dict(
        domain=[{"name": "d0", "lat": {"start": 50, "end": 55, "system": "values"},
                 "lon": {"start": 40, "end": 42, "system": "values"},
                 "time": {"start": "1980-01-01", "end": "1981-12-31", "crs": "timestamps"}}],
        input=[{"uri": mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}],
        operation=[ { "epa": "test.subset", "input": "v0"} ]
    )
    task: Task = client.request( "exe", request )
    time.sleep(1.5)
    status = task.status()
    print ( "status response = " + str(status))

