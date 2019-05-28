from stratus.handlers.base import Handler
from stratus.app.client import StratusClient
from .client import CeleryClient
from stratus.app.core import StratusCore
from .app import StratusApp
import os

class ServiceHandler( Handler ):

    def __init__(self, **kwargs ):
        htype = os.path.basename(os.path.dirname(__file__))
        super(ServiceHandler, self).__init__( htype, **kwargs )

    def newClient(self, core: StratusCore, **kwargs) -> StratusClient:
        return CeleryClient( core, **kwargs )

    def newApplication(self, core: StratusCore, **kwargs ) -> StratusApp:
        return StratusApp( core )


