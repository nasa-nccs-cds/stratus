from stratus.handlers.client import StratusClient, stratusrequest
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
import traceback, time, logging, xml, json, requests
from stratus.util.config import Config, StratusLogger, UID
from threading import Thread
from stratus.util.parsing import s2b, b2s
from stratus_endpoint.handler.base import Task, Status, TaskResult
from stratus.handlers.core import StratusCore
import random, string, os, pickle, queue
from stratus.handlers.rest.api.wps.wps_request import WPSExecuteRequest
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
        self.host_address = self["host_address"]
        self.wpsRequest = WPSExecuteRequest(self.host_address)

    @stratusrequest
    def request( self, requestSpec: Dict, **kwargs ) -> Task:
        response =  self.wpsRequest.exe(requestSpec)
        self.log( "Got response: " + str(response) )
        return RestTask( requestSpec['rid'], self.cid, response )

    def capabilities(self, type: str, **kwargs ) -> Dict:
        results = self.wpsRequest.getCapabilities()
        return results

    def log(self, msg: str ):
        self.logger.info( "[RP] " + msg )

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        if self.active:
            self.active = False


class RestTask(Task):

    def __init__(self, rid: str, cid: str, response: Dict, **kwargs):
        super(RestTask, self).__init__( rid, cid, **kwargs )
        self.logger = StratusLogger.getLogger()
        self.response = response


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
    task: RestTask = client.request( request )
    print( task.response["xml"] )

