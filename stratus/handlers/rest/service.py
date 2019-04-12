from stratus.handlers.base import Handler
from stratus.app.client import StratusClient
from stratus.app.core import StratusCore
from stratus.handlers.rest.api.core.client import CoreRestClient
from stratus.handlers.rest.api.wps.client import WPSRestClient
from stratus.handlers.rest.app import StratusApp
import os

MB = 1024 * 1024

class ServiceHandler( Handler ):

    def __init__(self, **kwargs ):
        htype = os.path.basename(os.path.dirname(__file__))
        super(ServiceHandler, self).__init__( htype, **kwargs )

    def newClient( self, cid = None, gateway=False ) -> StratusClient:
        API = self.parm("API","core").lower()
        cparms = {"cid": cid, **self.parms} if cid else self.parms
        if API == "core": return CoreRestClient( gateway=gateway, **cparms  )
        if API == "wps":  return WPSRestClient( gateway=gateway, **cparms )
        raise Exception( "Unrecognized API in REST ServiceHandler: " + API)

    def newApplication(self, core: StratusCore ) -> StratusApp:
        return StratusApp( core )
