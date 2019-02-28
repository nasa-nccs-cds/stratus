from stratus.handlers.client import StratusClient
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
import traceback, time, logging, xml, json, requests
from stratus.util.config import Config, StratusLogger, UID
from threading import Thread
from stratus.util.parsing import s2b, b2s
from stratus_endpoint.handler.base import Task, Status, TaskResult
import random, string, os, pickle, queue
import xarray as xa
from enum import Enum
MB = 1024 * 1024

class MessageState(Enum):
    ARRAY = 0
    FILE = 1
    RESULT = 2

class RestClient(StratusClient):

    def __init__( self, host_address, **kwargs ):
        super(RestClient, self).__init__( "rest", **kwargs )
        self.host = host_address
        self.response_manager = ResponseManager( self,  **kwargs )
        self.response_manager.start()

    def request(self, type: str, request: Dict, **kwargs ) -> Task:
        response = self.postMessage( type, request, **kwargs )
        self.log( "Got response: " + str(response) )
        return restTask( request['sid'], self.response_manager )

    def status(self, **kwargs ) -> Dict:
        return self.getMessage( "status", kwargs )

    def capabilities(self, type: str, **kwargs ) -> Dict:
        return self.getMessage( type, kwargs )

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

    def getMessage(self, type: str, request: Dict, **kwargs ) -> Dict:
        rid = request.get( "id", UID.randomId(6) )
        submissionId = self.sid(rid)
        request["sid"] = submissionId
        response: requests.Response = requests.get( f"{self.host}/{type}", params=request )
        print( f"RESPONSE({response.url}): {str(response)}: {response.text}"  )
        result = response.json()
        result["id"] = submissionId
        return result

    def postMessage(self, type: str, request: Dict, **kwargs ) -> Dict:
        rid = request.get( "id", UID.randomId(6) )
        submissionId = self.sid(rid)
        request["sid"] = submissionId
        response: requests.Response = requests.post( f"{self.host}/{type}", data=request )
        print( f"RESPONSE({response.url}): {str(response)}: {response.text}"  )
        if( response.ok ):
            result = response.json()
            result["id"] = submissionId
        else:
            result = {   "error": response.status_code,
                         "message": response.text,
                         "id": submissionId           }
        return result

    def waitUntilDone(self):
        self.response_manager.join()

class ResponseManager(Thread):

    def __init__(self, client: RestClient, **kwargs ):
        Thread.__init__(self)
        self.logger = StratusLogger.getLogger()
        self.client = client
        self.active = True
        self.setName('STRATUS zeromq client Response Thread')
        self.setDaemon(True)
        self.cacheDir = kwargs.get( "cacheDir",  os.path.expanduser( "~/.edas/cache") )
        self.log("Created RM, cache dir = " + self.cacheDir )
        self.poll_freq = kwargs.get( "poll_freq", 1.0 )
        self.statusMap: Dict[str,Status] = {}


    def getResult( self, block=True, timeout=None ) -> Optional[TaskResult]:
        try:                 return self.cached_results.get( block, timeout )
        except queue.Empty:  return None

    def run(self):
        try:
            self.log("Run RM thread")
            while( self.active ):
                statMap = self.client.status()
                for sid, stat in statMap.values(): self.statusMap[sid] = Status.decode(stat)
                time.sleep( self.poll_freq )

        except Exception as err:
            self.log( "ResponseManager error: " + str(err) )
            self.statusMap.clear()

    def getStatus( self, sid ) -> Status:
        return self.statusMap.get( sid, Status.UNKNOWN )

    def log(self, msg: str ):
        self.logger.info( "[RM] " + msg )

class restTask(Task):

    def __init__(self, sid: str, manager: ResponseManager, **kwargs):
        super(restTask, self).__init__( sid, **kwargs )
        self.logger = StratusLogger.getLogger()
        self.manager = manager

    def getResult(self, block=True, timeout=None ) ->  Optional[TaskResult]:
        return self.manager.getResult(block,timeout)

    def status(self) ->  Status:
        return self._status


if __name__ == "__main__":
    from stratus.util.test import TestDataManager as mgr
    client = RestClient( "http://127.0.0.1:5000/core" )
    client.init()
    request = dict(
        domain=[{"name": "d0", "lat": {"start": 50, "end": 55, "system": "values"},
                 "lon": {"start": 40, "end": 42, "system": "values"},
                 "time": {"start": "1980-01-01", "end": "1981-12-31", "crs": "timestamps"}}],
        input=[{"uri": mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}],
        operation=[ { "epa": "test.subset", "input": "v0"} ]
    )
    response0 = client.request( "exe", request )
    response1 = client.status()
    print ( "status response = " + str(response1))

