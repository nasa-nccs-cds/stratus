from stratus.handlers.client import StratusClient, stratusrequest
from stratus_endpoint.handler.base import Task, Status, Endpoint
from stratus_endpoint.handler.test import TestEndpoint
from stratus.handlers.endpoint.client import DirectClient
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple
import importlib, traceback

class TestClient(DirectClient):

    def __init__( self, **kwargs ):
        super(TestClient, self).__init__( **kwargs )
        self._epas = [ "test" ]


    def instantiateEndpoint(self):
        return TestEndpoint( **self.parms )


    def capabilities(self, type: str, **kwargs  ) -> Dict:
        if type == "epas":
            return dict( epas = self._epas )

if __name__ == "__main__":
    dc = TestClient()
    dc.instantiateEndpoint()
