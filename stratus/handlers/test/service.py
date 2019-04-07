import os
from stratus.handlers.base import Handler
from app.client import StratusClient
from app.base import StratusAppBase
from app.core import StratusCore
from .client import TestClient

class ServiceHandler( Handler ):

    def __init__(self, **kwargs ):
        htype = os.path.basename(os.path.dirname(__file__))
        super(ServiceHandler, self).__init__( htype, **kwargs )

    def newClient( self, gateway=False ) -> StratusClient:
        return TestClient( gateway=gateway, **self.parms )

    def newApplication(self, core: StratusCore ) -> StratusAppBase:
        raise Exception( "Can't stand up a stratus app for an test endpoint")






