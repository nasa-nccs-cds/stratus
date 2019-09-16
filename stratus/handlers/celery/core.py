from stratus.app.core import StratusCore
from typing import List, Union, Dict, Set, Iterator, Tuple, ItemsView
from stratus_endpoint.util.config import StratusLogger
import subprocess, os
from threading import Thread

class TaskManager(Thread):

    def __init__(self, name: str, spec: Dict[str,str] ):
        Thread.__init__(self)
        self._name = name
        self._spec = spec
        self.task = None
        self._completedProcess = None

    def run(self):
        from .app import celery_execute
        self.task = celery_execute.s( self._spec )
        self._completedProcess = subprocess.run(['celery', '--app=stratus.handlers.celery.app:app', 'worker', '-l', 'info',  '-Q', self._name ], check = True )


class CeleryCore( StratusCore ):

    def __init__(self, configSpec: Union[str,Dict[str,Dict]], **kwargs ):
        StratusCore.__init__( self, configSpec, internal_clients=False, **kwargs )
        self.logger = StratusLogger.getLogger()
        self._workers: Dict[str,TaskManager] = None
        self.baseDir = os.path.dirname(__file__)

    def buildWorker( self, name: str, spec: Dict[str,str] ):
        if name not in self._workers:
            self._startWorker( name, spec )

    def _startWorker(self, name: str, spec: Dict[str,str] ):
        taskManager = TaskManager( name, spec )
        self._workers[ name ] = taskManager
        try:
            taskManager.start()
        except subprocess.CalledProcessError as err:
            self.logger.error( f" Worker exited with error: {err.stderr}")
