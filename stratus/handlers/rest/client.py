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

    def request(self, type: str, request: Dict, **kwargs ) -> Task:
        response = self.postMessage( type, request, **kwargs )
        self.log( "Got response: " + str(response) )
        response_manager = ResponseManager( response["id"], self.host,   **kwargs )
        response_manager.start()
        return restTask(response_manager)

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
        submissionId = self.clientID + rid
        request["sid"] = submissionId
        response: requests.Response = requests.get( f"{self.host}/{type}", params=request )
        print( f"RESPONSE({response.url}): {str(response)}: {response.text}"  )
        result = response.json()
        result["id"] = submissionId
        return result

    def postMessage(self, type: str, request: Dict, **kwargs ) -> Dict:
        rid = request.get( "id", UID.randomId(6) )
        submissionId = self.clientID + rid
        request["sid"] = submissionId
        response: requests.Response = requests.post( f"{self.host}/{type}", json=request )
        print( f"RESPONSE({response.url}): {str(response)}: {response.text}"  )
        result = response.json()
        result["id"] = submissionId
        return result

    def waitUntilDone(self):
        self.response_manager.join()

class ResponseManager(Thread):

    def __init__(self, subscribeId: str, host: str, **kwargs ):
        Thread.__init__(self)
        self.logger = StratusLogger.getLogger()
        self.host = host
        self.subscribeId = subscribeId
        self.active = True
        self.mstate = MessageState.RESULT
        self.setName('STRATUS zeromq client Response Thread')
        self.cached_results: queue.Queue[TaskResult] = queue.Queue()
        self.setDaemon(True)
        self.cacheDir = kwargs.get( "cacheDir",  os.path.expanduser( "~/.edas/cache") )
        self.log("Created RM, cache dir = " + self.cacheDir )
        self.poll_freq = kwargs.get( "poll_freq", 1.0 )

    def cacheResult(self, header: Dict, data: Optional[xa.Dataset] ):
        self.cached_results.put( TaskResult(header,data)  )

    def getResult( self, block=True, timeout=None ) -> Optional[TaskResult]:
        try:                 return self.cached_results.get( block, timeout )
        except queue.Empty:  return None

    def run(self):
        response_socket = None
        try:
            self.log("Run RM thread")
            while( self.active ):
                self.processNextResponse()
                time.sleep( self.poll_freq )

        except Exception as err:
            self.log( "ResponseManager error: " + str(err) )
            self.cacheResult( { "error": str(err) }, None )
        finally:
            if response_socket: response_socket.close()

    def term(self):
        self.log("Terminate RM thread")
        if self.active:
            self.active = False

    def log(self, msg: str, maxPrintLen = 300 ):
        self.logger.info( "[RM] " + msg )

    def processNextResponse( self ):
        try:
            response = requests.get( self.host, params=dict( type="status") ).json()
            self.log(f"Received Status Response: " + str( response ) )
        except Exception as err:
            self.log( "EDAS error: {0}\n{1}\n".format(err, traceback.format_exc() ), 1000 )


class restTask(Task):

    def __init__(self, manager: ResponseManager, **kwargs):
        super(restTask, self).__init__(manager.subscribeId, **kwargs)
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
        operation=[ { "epa": "edas.subset", "input": "v0"} ]
    )
    response = client.request( "exe", request )
    print ( "response = " + str( response ) )

