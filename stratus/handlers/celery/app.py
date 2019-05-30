from stratus.app.base import StratusEmbeddedApp
from stratus.app.core import StratusCore
from stratus.app.client import stratusrequest
from stratus_endpoint.util.config import StratusLogger
from stratus_endpoint.handler.base import TaskHandle, TaskResult
from celery import Celery
from typing import Dict, List
import queue, datetime

app = Celery( "stratus", backend='redis://localhost', broker='pyamqp://' )

class StratusApp(StratusEmbeddedApp):

    def __init__( self, core: StratusCore ):
        StratusEmbeddedApp.__init__(self, core)
        self.logger =  StratusLogger.getLogger()
        self.active = True
        self.parms = self.getConfigParms('stratus')

    def run(self):
        pass


    def init(self, **kwargs):
        pass   # get applicatoin object.

    @stratusrequest
    def request(self, requestSpec: Dict, inputs: List[TaskResult] = None, **kwargs ) -> TaskHandle:
        pass

    def capabilities(self, ctype: str, **kwargs ) -> Dict:
        return {}

    def log(self, msg: str ):
        self.logger.info( "[ZP] " + msg )

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        if self.active:
            self.active = False
