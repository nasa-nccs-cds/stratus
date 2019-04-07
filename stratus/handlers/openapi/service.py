from stratus.handlers.base import Handler
from app.client import StratusClient
from .client import OpenApiClient
import os

class ServiceHandler( Handler ):

    def __init__(self, **kwargs ):
        htype = os.path.basename(os.path.dirname(__file__))
        super(ServiceHandler, self).__init__( htype, **kwargs )

    def newClient( self, gateway=False ) -> StratusClient:
        return OpenApiClient( gateway=gateway, **self.parms  )
