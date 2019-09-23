from stratus.app.core import StratusCore
from typing import List, Union, Dict, Set, Iterator, Tuple, ItemsView
from stratus_endpoint.util.config import StratusLogger
import subprocess, os
from threading import Thread

class TaskManager(Thread):

    def __init__(self, name: str ):
        Thread.__init__(self)
        self._name = name
        self._completedProcess = None

    def run(self):
        self._completedProcess = subprocess.run(['celery', '--app=stratus.handlers.celery.app:app', 'worker', '-l', 'info',  '-Q', self._name, '-E' ], check = True )

class FlowerManager(Thread):

    def __init__ (self ):
        Thread.__init__(self)
        self._completedProcess = None
        self.logger = StratusLogger.getLogger()

    def run(self):
        try:
            self._completedProcess = subprocess.run(['celery', 'flower', '--app=stratus.handlers.celery.app:app', '--port=5555', '--address=127.0.0.1' ], check = True )
        except Exception as err:
            self.logger.error( f"Error staring Celery: {err}" )

class CeleryCore( StratusCore ):

    def __init__(self, configSpec: Union[str,Dict[str,Dict]], **kwargs ):
        self._workers: Dict[str,TaskManager] = {}
        self._flower = None
        self.baseDir = os.path.dirname(__file__)
        StratusCore.__init__( self, configSpec, internal_clients=False, **kwargs )
        self.parms.update( type="celery" )
        self.logger = StratusLogger.getLogger()
        self.logger.info( f"Starting CeleryCore with parms: {self.parms}" )
        if self.parm( 'flower', False ): self._startFlower()

    def buildWorker( self, name: str, spec: Dict[str,str] ):
        if name not in self._workers:
            self._startWorker( name )

    def _startWorker(self, name: str ):
        taskManager = TaskManager( name )
        self._workers[ name ] = taskManager
        try:
            taskManager.start()
        except subprocess.CalledProcessError as err:
            self.logger.error( f" Worker exited with error: {err.stderr}")

    def _startFlower(self):
        if self._flower is None:
            self.logger.info( "Starting Flower")
            self._flower = FlowerManager()
            self._flower.start()