from stratus.handlers.client import StratusClient, stratusrequest
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
import traceback, time, logging, xml, json, requests
from stratus.util.config import Config, StratusLogger, UID
from threading import Thread
from stratus.util.parsing import s2b, b2s
from stratus_endpoint.handler.base import Task, Status, TaskResult
from stratus.handlers.core import StratusCore
import random, string, os, pickle, queue
import xarray as xa
from enum import Enum
MB = 1024 * 1024

class MessageState(Enum):
    ARRAY = 0
    FILE = 1
    RESULT = 2

class CoreRestClient(StratusClient):

    def __init__( self, **kwargs ):
        super(CoreRestClient, self).__init__( "rest", **kwargs )
        if "host_address" in self.parms:
            self.host_address = self["host_address"]
        else:
            host = self["host"]
            port = self["port"]
            route = self["route"]
            self.host_address = f"http://{host}:{port}/{route}"
        self.response_manager = ResponseManager.getManger( self.cid, self.host_address )
        self.response_manager.start()

    @stratusrequest
    def request( self, requestSpec: Dict, **kwargs ) -> Task:
        response = self.response_manager.postMessage( "exe", requestSpec, **kwargs )
        self.log( "Got response: " + str(response) )
        return RestTask( requestSpec['rid'], self.cid, self.response_manager )

    def status(self, **kwargs ) -> Status:
        result = self.response_manager.getMessage( "status", {}, **kwargs)
        return result[kwargs.get("rid")]

    def capabilities(self, type: str, **kwargs ) -> Dict:
        result = self.response_manager.getMessage( "capabilities", {"type":type}, **kwargs )
        rtype = result["type"]
        if rtype == "error":    raise Exception( result["message"] )
        else:                   return result["json"]

    def log(self, msg: str ):
        self.logger.info( "[RP] " + msg )

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

    def __init__(self, cid: str, host_address: str, **kwargs ):
        Thread.__init__(self)
        self.logger = StratusLogger.getLogger()
        self.host_address = host_address
        self.active = True
        self.cid = cid
        self.debug = False
        self.setName('STRATUS zeromq client Response Thread')
        self.setDaemon(True)
        self.poll_freq = kwargs.get( "poll_freq", 0.5 )
        self.timeout = kwargs.get("timeout", 60.0)
        self.statusMap: Dict[str,Status] = {}

    @classmethod
    def getManger( cls, cid: str, host_address: str)  ->  "ResponseManager":
        return cls.managers.setdefault(host_address, ResponseManager( cid, host_address ) )

    def run(self):
        debug = False
        try:
            self.log("Run RM thread")
            while( self.active ):
                statMap = self._getStatusMap()
                for key,value in statMap.items(): self.statusMap[key] = Status.decode( value )
                if debug: self.logger.info( "Server Job Status: " + str( statMap ) + ";  Client Job Status: " + str( self.statusMap ) )
                time.sleep( self.poll_freq )

        except Exception as err:
            self.log( "ResponseManager error: " + str(err) )
            self.log( traceback.format_exc() )
            self.statusMap.clear()

    def updateStatus(self, message: Dict ) -> Dict:
        if "status" in message:
            rid = message["rid"]
            status = Status.decode( message["status"] )
            self.statusMap[ rid ] = status
            message["status"] = status
            if self.debug: self.logger.info( f"REST_CLIENT: Update Status Map[{rid}]: " + str( status ) )
        return message

    def getMessage(self, type: str, requestSpec: Dict, **kwargs ) -> Dict:
        request_params = dict(requestSpec)
        request_params.update(kwargs)
        address = f"{self.host_address}/{type}"
        if self.debug: self.log(f"REQUEST[{address}](status): {str(request_params)}")
        response: requests.Response = requests.get( address, params=request_params )
        if self.debug: self.log( f"RESPONSE[{response.encoding}]({response.url}): {str(response)}: {response.text}"  )
        return self.processResponse(response)

    def postMessage(self, type: str, requestSpec: Dict, **kwargs ) -> Dict:
        request_params = dict(requestSpec)
        request_params.update(kwargs)
        self.logger.info( f"POSTing request: {str(requestSpec)}")
        response: requests.Response = requests.post( f"{self.host_address}/{type}", json=requestSpec )
        self.log( f"RESPONSE[{response.encoding}]({response.url}): {str(response)}: {response.text}"  )
        return self.processResponse(response)

    def processResponse(self, response: requests.Response)-> Dict:
        if( response.ok ):
            if self.debug: self.logger.info( f"PROCESS REPSONSE: headers = {str(response.headers)}")
            content_type = response.headers.get('Content-Type',None)
            if content_type == "application/octet-stream":  result = { "type": "data", "header": response.headers, "content": pickle.loads( response.content ) }
            else:                                           result = { "type": "json",  "json": self.updateStatus( response.json() ) }
        else:                                               result = { "type": "error",  "code": response.status_code, "message": response.text }
        return result

    def getResult(self, rid: str, block=True, timeout=None  ) ->  Optional[TaskResult]:
        if block: self.waitUntilReady( rid, timeout )
        result = self.getMessage( "result", dict(rid=rid) )
        rtype = result["type"]
        if   rtype == "error":  raise Exception( result["message"] )
        elif rtype == "json":   return TaskResult( result["json"] )
        elif rtype == "data":   return result.get("content",None)
        else:                   raise Exception( f"Unrecognized result type: {rtype}")

    def _getStatusMap(self) -> Dict:
        result = self.getMessage( "status", {}, timeout=self.timeout  )
        rtype = result["type"]
        if rtype == "error":    raise Exception( result["message"] )
        else:                   return result["json"]

    def getStatus( self, rid ) -> Status:
        status = self.statusMap.get( rid, Status.UNKNOWN )
        if self.debug: self.logger.info( f"Status[{rid}]: {status}")
        return status

    def completed(self, rid ) -> bool :
        status = self.getStatus(rid)
        result =  status in [Status.COMPLETED, Status.ERROR ]
        if self.debug: self.logger.info( f"COMPLETED[{status}]: {result}")
        return result

    def waitUntilReady( self, rid: str, timeout: float = None ):
        accum_time = 0.0
        while not self.completed( rid ):
            time.sleep(0.2)
            if timeout is not None:
                accum_time += 0.2
                if( accum_time >= timeout ):
                    return False
        self.logger.info("[RM] Result Available" )
        return True

    def log(self, msg: str ):
        self.logger.info( "[RM] " + msg )

    def term(self):
        self.active = False

class RestTask(Task):

    def __init__(self, rid: str, cid: str, manager: ResponseManager, **kwargs):
        super(RestTask, self).__init__( rid, cid, **kwargs )
        self.logger = StratusLogger.getLogger()
        self.manager: ResponseManager = manager

    def getResult(self, block=True, timeout=None ) ->  Optional[TaskResult]:
        return self.manager.getResult( self.rid, block, timeout )

    def status(self) ->  Status:
        return self.manager.getStatus( self.rid )


if __name__ == "__main__":
    from stratus.util.test import TestDataManager as mgr
    HERE = os.path.dirname(os.path.abspath(__file__))
    SETTINGS_FILE = os.path.join(HERE, "client_test_settings.ini")

    core = StratusCore( SETTINGS_FILE )
    client = core.getClient()
    client.init()
    request = dict(
        domain=[{"name": "d0", "lat": {"start": 50, "end": 55, "system": "values"},
                 "lon": {"start": 40, "end": 42, "system": "values"},
                 "time": {"start": "1980-01-01", "end": "1981-12-31", "crs": "timestamps"}}],
        input=[{"uri": mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}],
        operation=[ { "epa": "test.subset", "input": "v0"} ]
    )
    task: Task = client.request( request )
    time.sleep(1.5)
    status = task.status()
    print ( "status response = " + str(status))

