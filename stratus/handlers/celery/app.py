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
import queue, traceback
from celery.utils.log import get_task_logger
from celery import Task
logger = get_task_logger(__name__)

class CeleryApp( Celery ):

    def __init__( self, *args, **kwargs ):
        Celery.__init__( *args, **kwargs  )
        self.stratusApp: StratusAppCelery = None

app = CeleryApp( 'stratus', broker = 'redis://localhost', backend = 'redis://localhost' )

app.conf.update(
    result_expires=3600,
    task_serializer = 'pickle',
    accept_content = ['json', 'pickle', 'application/x-python-serialize'],
    result_serializer = 'pickle'
)

class CeleryTask(Task):
    def __init__(self):
        Task.__init__(self)
        self._handlers: Handlers = None
        self._name: str = None
        self._handler: Handler = None

    def initHandler( self, clientSpec: Dict[str,Dict] ):
        if self._handlers is None:
            self.handlers = Handlers( None, clientSpec )
            self._name, handlerSpec = list(clientSpec.items())[0]
            self._handler = self.handlers.available[ self._name ]

@app.task( bind=True, base=CeleryTask )
def celery_execute( self, clientSpec: Dict, requestSpec: Dict, inputs: List[TaskResult] ) -> Optional[TaskResult]:
    cid = requestSpec['cid']
    self.initHandler( clientSpec )
    client: StratusClient = self._handler.getClient( cid )
    logger.info( f"Client[{cid}]: Executing request: {requestSpec}")
    taskHandle: TaskHandle = client.request( requestSpec, inputs )
    return taskHandle.getResult()

class StratusAppCelery(StratusEmbeddedApp):

    def getWorkflow( self, tasks: List[WorkflowTask] ) -> WorkflowBase:
        from handlers.celery.workflow import CeleryWorkflow
        return CeleryWorkflow(nodes=tasks)

    def processError(self, rid: str, ex: Exception): pass

    def initInteractions(self): pass

    def updateInteractions(self): pass