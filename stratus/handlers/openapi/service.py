from stratus.handlers.base import Handler
from stratus.handlers.client import StratusClient
from .client import OpenApiClient
import sys, inspect, os

class ServiceHandler( Handler ):

    def __init__(self, **kwargs ):
        htype = os.path.basename(os.path.dirname(__file__))
        super(ServiceHandler, self).__init__( htype, **kwargs )

    def newClient(self) -> StratusClient:
        return OpenApiClient( **self.parms )
