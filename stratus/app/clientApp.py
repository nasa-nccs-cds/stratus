from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from stratus_endpoint.util.config import Config, StratusLogger, UID
from stratus_endpoint.handler.base import TaskHandle, Status, TaskResult
from stratus.app.client import StratusClient, stratusrequest
from stratus.app.base import StratusAppBase
from stratus.app.core import StratusCore
import abc, fnmatch
from decorator import decorator, dispatch_on


class StratusAppClient(StratusClient):

    def __init__( self, core: StratusCore, type: str, **kwargs ):
        StratusClient.__init__( self, type, **kwargs )
        self.app: StratusAppBase = core.getApplication()

    def init(self, **kwargs): pass

    @stratusrequest
    def request(self, requestSpec: Dict, inputs: List[TaskResult] = None, **kwargs ) -> TaskHandle:
        self.app.requestQueue.put(requestSpec)
        task: Optional[TaskHandle] = self.app.getResult(rid)
        return task

    def capabilities(self, ctype: str, **kwargs ) -> Dict:
        return self.app.capabilities( ctype, **kwargs )

    def log(self, msg: str ):
        self.logger.info( "[ZP] " + msg )

    def __del__(self):
        self.app.shutdown()

    def shutdown(self):
        self.app.shutdown()