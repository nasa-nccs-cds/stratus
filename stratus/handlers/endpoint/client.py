from stratus.handlers.client import StratusClient
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple
import importlib

class DirectClient(StratusClient):

    def __init__( self, **kwargs ):
        super(DirectClient, self).__init__( "endpoint", **kwargs )
        self.endpoint = self.instantiateEndpoint()

    def instantiateEndpoint( self ):
        module_name = self["module"]
        class_name = self["class"]
        module = importlib.import_module(module_name)
        epclass = getattr(module, class_name)
        return epclass( **self.parms )

    def request(self, epa: str, **kwargs ) -> Dict:
        if epa == "epas":
            return self.endpoint.epas()
        else:
            return self.endpoint.request(epa, **kwargs )

    def init( self ): self.endpoint.init()
