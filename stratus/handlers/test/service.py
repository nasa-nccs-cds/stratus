import os
from stratus.handlers.base import Handler
from stratus.app.client import StratusClient
from stratus.app.base import StratusAppBase, TestStratusApp
from stratus.app.core import StratusCore
from .client import TestClient

class ServiceHandler( Handler ):

    def __init__(self, **kwargs ):
        htype = os.path.basename(os.path.dirname(__file__))
        super(ServiceHandler, self).__init__( htype, **kwargs )

    def newClient( self, core: StratusCore, **kwargs ) -> StratusClient:
        return TestClient( **kwargs )

    def newApplication(self, core: StratusCore, **kwargs ) -> StratusAppBase:
        return TestStratusApp( core )






