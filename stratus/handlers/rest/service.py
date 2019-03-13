from stratus.handlers.base import Handler
from stratus.handlers.client import StratusClient
from stratus.handlers.core import StratusCore
from handlers.rest.api.core.client import CoreRestClient
from handlers.rest.api.wps.client import WPSRestClient
from .app import StratusApp
import os

MB = 1024 * 1024

class ServiceHandler( Handler ):

    def __init__(self, **kwargs ):
        htype = os.path.basename(os.path.dirname(__file__))
        super(ServiceHandler, self).__init__( htype, **kwargs )

    def newClient(self) -> StratusClient:
        API = self.parm("API","core").lower()
        if API == "core": return CoreRestClient( **self.parms )
        if API == "wps":  return WPSRestClient(**self.parms)
        raise Exception( "Unrecognized API in REST ServiceHandler: " + API)

    def newApplication(self, core: StratusCore ) -> StratusApp:
        return StratusApp( core )
