import os
from stratus.handlers.base import Handler
from app.client import StratusClient
from app.base import StratusAppBase
from app.core import StratusCore
from .client import DirectClient

class ServiceHandler( Handler ):

    def __init__(self, **kwargs ):
        htype = os.path.basename(os.path.dirname(__file__))
        super(ServiceHandler, self).__init__( htype, **kwargs )

    def newClient( self, gateway=False ) -> StratusClient:
        return DirectClient( gateway=gateway, **self.parms )

    def newApplication(self, core: StratusCore ) -> StratusAppBase:
        raise Exception( "Can't stand up a stratus app for an endpoint")






