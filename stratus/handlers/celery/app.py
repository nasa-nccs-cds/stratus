from stratus.app.core import StratusCore
from stratus.app.base import StratusAppBase, StratusEmbeddedApp
from stratus.app.client import stratusrequest
from stratus.app.operations import WorkflowBase
from stratus_endpoint.util.config import StratusLogger, UID
from stratus_endpoint.handler.base import TaskHandle, TaskResult
from stratus.app.operations import StratusWorkflow, WorkflowTask
from stratus.app.client import StratusClient
from stratus.handlers.manager import Handlers
from stratus.handlers.base import Handler
from celery import Celery
from typing import Dict, List, Optional
import queue, traceback, logging, os
from celery.utils.log import get_task_logger
from celery import Task
from celery.signals import after_setup_task_logger
from celery.app.log import TaskFormatter

logger = get_task_logger(__name__)
app = Celery( 'stratus', broker = 'redis://localhost', backend = 'redis://localhost' )

app.conf.update(
    result_expires=3600,
    task_serializer = 'pickle',
    accept_content = ['json', 'pickle', 'application/x-python-serialize'],
    result_serializer = 'pickle'
)
celery_log_file = os.path.expanduser("~/.stratus/logs/celery.log")
app.log.setup_logging_subsystem( loglevel=logging.INFO, logfile=celery_log_file, format='[%(asctime)s: %(levelname)s/%(processName)s-> %(pathname)s:%(lineno)d]: %(message)s' )


class CeleryTask(Task):
    def __init__(self):
        Task.__init__(self)
        self._handlers: Handlers = None
        self._name: str = None
        self.core: StratusCore = None
        self._handler: Handler = None

    def initHandler( self, clientSpec: Dict[str,Dict] ):
        if self._handlers is None:
            hspec: Dict[str,Dict] = { clientSpec['name']: clientSpec, "stratus": { 'type': "celery", 'name':"stratus" } }
            logger.info(f"Init Celery Task Handler with spec: {hspec}")
            self.core = StratusCore( hspec )
            self._handlers = Handlers( self.core, hspec )
            self._name, handlerSpec = list(hspec.items())[0]
            self._handler: Handler = self._handlers.available[ self._name ]

@app.task( bind=True, base=CeleryTask )
def celery_execute( self, inputs: List[TaskResult], clientSpec: Dict, requestSpec: Dict ) -> Optional[TaskResult]:
    cid = clientSpec.get('cid',"UNKNOWN")
    logger.info( f"Client[{cid}]: Executing request: {requestSpec}" )
    self.initHandler( clientSpec )
    client: StratusClient = self._handler.client( self.core )
    taskHandle: TaskHandle = client.request( requestSpec, inputs )
    return taskHandle.getResult( block=True ) if taskHandle else None

class StratusAppCelery(StratusEmbeddedApp):

    def getWorkflow( self, tasks: List[WorkflowTask] ) -> WorkflowBase:
        from stratus.handlers.celery.workflow import CeleryWorkflow
        return CeleryWorkflow( nodes=tasks, **self.parms )

    def processError(self, rid: str, ex: Exception): pass

    def initInteractions(self): pass

    def updateInteractions(self): pass
