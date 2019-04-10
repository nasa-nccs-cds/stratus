from stratus.handlers.endpoint.test import TestEndpoint
from stratus.handlers.endpoint.client import DirectClient
from typing import Dict


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
