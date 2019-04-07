from stratus.handlers.base import Handler
from app.client import StratusClient
from .client import ZMQClient
from app.core import StratusCore
from .app import StratusApp
import os

MB = 1024 * 1024

class ServiceHandler( Handler ):

    def __init__(self, **kwargs ):
        htype = os.path.basename(os.path.dirname(__file__))
        super(ServiceHandler, self).__init__( htype, **kwargs )

    def newClient( self, gateway=False ) -> StratusClient:
        return ZMQClient( gateway=gateway, **self.parms )

    def newApplication(self, core: StratusCore ) -> StratusApp:
        return StratusApp( core )


