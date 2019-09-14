from stratus.app.base import StratusEmbeddedApp
from stratus.app.core import StratusCore
from stratus.app.client import stratusrequest
from stratus_endpoint.util.config import StratusLogger
from stratus_endpoint.handler.base import TaskHandle, TaskResult
from stratus.app.operations import Workflow, WorkflowTask
from stratus.app.client import StratusClient
from celery import Celery
from typing import Dict, List, Optional
import queue, datetime
from celery.utils.log import get_task_logger
from celery import Task
logger = get_task_logger(__name__)

class StratusCeleryApp( Celery ):

    def __init__( self, *args, **kwargs ):
        Celery.__init__( *args, **kwargs  )
        self.stratusApp: StratusApp = None

app = StratusCeleryApp( 'stratus', broker = 'redis://localhost', backend = 'redis://localhost' )

app.conf.update(
    result_expires=3600,
    task_serializer = 'pickle',
    accept_content = ['json', 'pickle', 'application/x-python-serialize'],
    result_serializer = 'pickle'
)

class CeleryTask(Task):
    def getClient( self, cid: str ) -> StratusClient:
        core: StratusCore = app.stratusApp.core
        return core.handlers.     #  getClients()

@app.task( bind=True, base=CeleryTask )
def celery_execute( self, cid, requestSpec: Dict, inputs: List[TaskResult] ) -> Optional[TaskResult]:
    client: StratusClient = self.getClient(cid)
    logger.info( f"Client[cid]: Executing request: {requestSpec}")
    taskHandle: TaskHandle = client.request( requestSpec, inputs )
    return taskHandle.getResult()

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

    def getWorkflow( self, tasks: List[WorkflowTask] ) -> Workflow:
        return CeleryWorkflow(nodes=tasks)


