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

    def __init__( self, host_address, **kwargs ):
        super(RestClient, self).__init__( "rest", **kwargs )
        self.host = host_address
        self.response_manager = ResponseManager.getManger( host_address )
        self.response_manager.start()

    def request(self, type: str, request: Dict, **kwargs ) -> Task:
        rid = request.get("id",None)
        response = self.response_manager.postMessage( self.sid(rid), type, request, **kwargs )
        self.log( "Got response: " + str(response) )
        return restTask( request['sid'], self.response_manager )

    def status(self, **kwargs ) -> Dict:
        return self.response_manager.getMessage( self.sid(), "status", kwargs )

    def capabilities(self, type: str, **kwargs ) -> Dict:
        return self.response_manager.getMessage( self.sid(), type, kwargs )

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
        try:
            self.log("Run RM thread")
            while( self.active ):
                statMap = self._getStatusMap()
                for sid,stat in statMap.items(): self.statusMap[sid] = Status.decode(stat)
                time.sleep( self.poll_freq )

        except Exception as err:
            self.log( "ResponseManager error: " + str(err) )
            self.log( traceback.format_exc() )
            self.statusMap.clear()

    def getMessage(self, sid: str, type: str, request: Dict, **kwargs ) -> Dict:
        request["sid"] = sid
        response: requests.Response = requests.get( f"{self.host_address}/{type}", params=request )
        print( f"RESPONSE[{response.encoding}]({response.url}): {str(response)}: {response.text}"  )
        return self.unpackResponse( sid, response )

    def postMessage(self, sid: str, type: str, request: Dict, **kwargs ) -> Dict:
        request["sid"] = sid
        self.logger.info( f"POSTing request: {str(request)}")
        response: requests.Response = requests.post( f"{self.host_address}/{type}", json=request )
        print( f"RESPONSE[{response.encoding}]({response.url}): {str(response)}: {response.text}"  )
        return self.unpackResponse( sid, response )

    def unpackResponse(self, sid: str, response: requests.Response)-> Dict:
        if( response.ok ):
            if response.encoding == "application/octet-stream":  result = { "content": pickle.loads( response.content ) }
            else:                                                result = response.json()
        else:                                                    result = {"error": response.status_code, "message": response.text}
        result["id"] = sid
        return result

    def getResult(self, sid: str, block=True, timeout=None  ) ->  Optional[TaskResult]:
        if block: self.waitUntilReady( sid, timeout )
        result = self.getMessage( sid, "result", {} )
        return result["content"]

    def _getStatusMap(self) -> Dict:
        response: requests.Response = requests.get( f"{self.host_address}/status", timeout=self.timeout )
        return response.json()

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

