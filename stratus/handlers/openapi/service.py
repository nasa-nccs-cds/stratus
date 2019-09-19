from stratus.handlers.base import Handler
from stratus.app.client import StratusClient
from stratus.app.core import StratusCore
from .client import OpenApiClient
import os

class ServiceHandler( Handler ):

    def __init__(self, **kwargs ):
        htype = os.path.basename(os.path.dirname(__file__))
        super(ServiceHandler, self).__init__( htype, **kwargs )

    def newClient( self, core: StratusCore, **kwargs ) -> StratusClient:
        return OpenApiClient( **kwargs  )
