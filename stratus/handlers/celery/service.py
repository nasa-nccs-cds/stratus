from stratus.handlers.base import Handler
from stratus.app.client import StratusClient
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
from .client import CeleryClient
from stratus.app.core import StratusCore
from stratus.app.base import StratusAppBase
from .app import StratusAppCelery
import os

class ServiceHandler( Handler ):

    def __init__(self, **kwargs ):
        htype = os.path.basename(os.path.dirname(__file__))
        super(ServiceHandler, self).__init__( htype, **kwargs )
        self.apps: Dict[str,StratusAppCelery] = {}

    def newClient(self, core: StratusCore, **kwargs) -> StratusClient:
        app = self.getApplication( core )
        return CeleryClient( app, **kwargs )

    def newApplication(self, core: StratusCore, **kwargs ) -> StratusAppBase:
        return self.getApplication( core )

    def getApplication(self, core: StratusCore, **kwargs ):
        if core.id not in self.apps:
            self.apps[core.id] = StratusAppCelery( core )
        return self.apps[core.id]


