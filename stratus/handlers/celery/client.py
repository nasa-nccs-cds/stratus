from celery import Celery
from stratus_endpoint.util.config import StratusLogger, UID
from threading import Thread
from typing import Dict, Optional, List
from stratus.util.parsing import s2b, b2s
from stratus_endpoint.handler.base import TaskHandle, Status, TaskResult
from stratus.app.client import StratusClient, stratusrequest
from stratus.app.core import StratusCore
import os, pickle, queue
import xarray as xa
from enum import Enum
MB = 1024 * 1024

class CeleryClient(StratusClient):

    def __init__( self, **kwargs ):
        super(CeleryClient, self).__init__( "celery", **kwargs )

    @stratusrequest
    def request(self, request: Dict, inputs: List[TaskResult] = None, **kwargs ) -> TaskHandle: pass

    def status(self, **kwargs ) -> Status: pass

    def capabilities(self, type: str, **kwargs ) -> Dict: pass
