from stratus.app.client import StratusClient, stratusrequest
import traceback, json
from celery import Celery
from stratus_endpoint.util.config import StratusLogger, UID
from threading import Thread
from typing import Dict, Optional, List
from stratus.util.parsing import s2b, b2s
from stratus_endpoint.handler.base import TaskHandle, Status, TaskResult
import os, pickle, queue
import xarray as xa
from enum import Enum
MB = 1024 * 1024

class CeleryClient(StratusClient):

    def __init__( self, **kwargs ):
        super(CeleryClient, self).__init__( "celery", **kwargs )
        self.app = None

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

