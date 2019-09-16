from stratus.handlers.base import Handler
from stratus.app.client import StratusClient
from .client import CeleryClient
from stratus.app.core import StratusCore
from .app import StratusAppCelery
import os

class ServiceHandler( Handler ):

    def __init__(self, **kwargs ):
        htype = os.path.basename(os.path.dirname(__file__))
        super(ServiceHandler, self).__init__( htype, **kwargs )

    def newClient(self, **kwargs) -> StratusClient:
        return CeleryClient( **kwargs )

    def newApplication(self, core: StratusCore, **kwargs ) -> StratusAppCelery:
        from .app import app
        stratusApp =  StratusAppCelery( core )
        app.stratusApp = stratusApp
        return stratusApp


