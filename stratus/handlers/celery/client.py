from celery import Celery
from stratus_endpoint.util.config import StratusLogger, UID
from threading import Thread
from typing import Dict, Optional, List
from stratus.util.parsing import s2b, b2s
from .app import StratusAppCelery
from stratus_endpoint.handler.base import TaskHandle, Status, TaskResult
from stratus.app.client import StratusClient, stratusrequest
from stratus.app.core import StratusCore
import os, pickle, queue
import xarray as xa
from enum import Enum
MB = 1024 * 1024

class CeleryClient(StratusClient):

    def __init__( self, app: StratusAppCelery, **kwargs ):
        super(CeleryClient, self).__init__( "celery", **kwargs )
        self._app = app

    @stratusrequest
    def request(self, request: Dict, inputs: List[TaskResult] = None, **kwargs ) -> TaskHandle:
        return self._app.handle_client_request(request, inputs, **kwargs)

    def status(self, **kwargs ) -> Status:
        from stratus.app.operations import StratusWorkflow
        rid = kwargs.get("rid")
        workflow: Optional[StratusWorkflow] = self._app.getWorkflow(rid)
        if workflow in None: return Status.UNKNOWN
        return workflow.status()

    def capabilities(self, type: str, **kwargs ) -> Dict:
        return self._app.capabilities( type, **kwargs )
