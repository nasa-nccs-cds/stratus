from stratus.app.base import StratusAppBase
from stratus.app.core import StratusCore
import json
from stratus_endpoint.util.config import StratusLogger
import traceback
from celery import Celery
from typing import Dict
import queue, datetime
from stratus.app.operations import WorkflowExeFuture

app = Celery( "stratus", backend='redis://localhost', broker='pyamqp://' )

class StratusApp(StratusAppBase):

    def __init__( self, core: StratusCore ):
        StratusAppBase.__init__(self, core )
        self.logger =  StratusLogger.getLogger()
        self.active = True
        self.parms = self.getConfigParms('stratus')

