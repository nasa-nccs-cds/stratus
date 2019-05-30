from stratus.handlers.base import Handler
from stratus.app.client import StratusClient
from .client import ZMQClient
from stratus.app.core import StratusCore
from .app import StratusApp
import os

MB = 1024 * 1024

class ServiceHandler( Handler ):

    def __init__(self, **kwargs ):
        htype = os.path.basename(os.path.dirname(__file__))
        super(ServiceHandler, self).__init__( htype, **kwargs )

    def newClient( self, core: StratusCore, **kwargs ) -> StratusClient:
        return ZMQClient( **kwargs )

    def newApplication(self, core: StratusCore, **kwargs ) -> StratusApp:
        return StratusApp( core, **kwargs )


