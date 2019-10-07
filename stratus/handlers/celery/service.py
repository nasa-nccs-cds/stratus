from stratus.handlers.base import Handler
from stratus.app.client import StratusClient
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
from .client import CeleryClient
from stratus.app.core import StratusCore
from stratus.app.base import StratusAppBase
from .app import StratusAppCelery
from stratus_endpoint.util.config import StratusLogger, UID
from stratus.util.parsing import str2bool
import subprocess, os
from threading import Thread

class TaskManager(Thread):

    def __init__(self, name: str ):
        Thread.__init__(self)
        self._name = name
        self.id = self._name
        self._completedProcess = None

    def run(self):
        self._completedProcess = subprocess.run(['celery', '--app=stratus.handlers.celery.app:app', 'worker', '-l', 'info',  '-Q', self._name,  '-n', self.id, '-E' ], check = True )

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

class ServiceHandler( Handler ):

    def __init__(self, **kwargs ):
        htype = os.path.basename(os.path.dirname(__file__))
        self._workers: Dict[str,TaskManager] = {}
        self._flower = None
        self.baseDir = os.path.dirname(__file__)
        super(ServiceHandler, self).__init__( htype, **kwargs )
        self._app: StratusAppCelery = None
        if str2bool( self.parm( 'flower', "false" ) ): self._startFlower()

    def newClient(self, core: StratusCore, **kwargs) -> StratusClient:
        app = self.getApplication( core )
        return CeleryClient( app, **kwargs )

    def newApplication(self, core: StratusCore, **kwargs ) -> StratusAppBase:
        return self.getApplication( core )

    def getCeleryCore(self, core: StratusCore, **kwargs ) -> StratusCore:
        for key, core_celery_params in core.config.items():
            if core_celery_params.get('type') == 'celery':
                celery_settings = core_celery_params.get( "settings")
                if celery_settings is not None:
                    return StratusCore(celery_settings)
        return core

    def getApplication(self, core: StratusCore, **kwargs ):
        if self._app is None:
            self._app = StratusAppCelery( self.getCeleryCore( core, **kwargs ) )
        return self._app

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