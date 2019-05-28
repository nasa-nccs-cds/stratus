from stratus.handlers.base import Handler
from stratus.app.client import StratusClient
from stratus.app.core import StratusCore
from stratus.handlers.rest.api.core.client import CoreRestClient
from stratus.handlers.rest.api.wps.client import WPSRestClient
from stratus.handlers.rest.api.ows_wps.client import OwsWpsClient
from stratus.handlers.rest.app import StratusApp
import os

MB = 1024 * 1024

class ServiceHandler( Handler ):

    def __init__(self, **kwargs ):
        htype = os.path.basename(os.path.dirname(__file__))
        super(ServiceHandler, self).__init__( htype, **kwargs )

    def newClient( self, core: StratusCore, **kwargs ) -> StratusClient:
        cid = kwargs.get("cid")
        gateway = kwargs.get("gateway")
        API = self.parm("API","core").lower()
        cparms = {"cid": cid, **self.parms} if cid else self.parms
        if API == "core":    return CoreRestClient( gateway=gateway, **cparms )
        if API == "wps":     return WPSRestClient(  gateway=gateway, **cparms )
        if API == "ows_wps": return OwsWpsClient(   gateway=gateway, **cparms )
        raise Exception( "Unrecognized API in REST ServiceHandler: " + API)

    def newApplication(self, core: StratusCore, **kwargs ) -> StratusApp:
        return StratusApp( core )
