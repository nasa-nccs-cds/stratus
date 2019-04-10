import os
from typing import Dict, List
from stratus_endpoint.handler.test import Endpoint, TaskHandle, TestTask

class TestEndpoint(Endpoint):

    def __init__( self, **kwargs ):
        Endpoint.__init__( self, **kwargs )
        self._epas = [ f"test{index}" for index in range(10) ]

    def request(self, requestSpec: Dict, **kwargs ) -> "TaskHandle":
        workTime = float( requestSpec.get( "workTime", 0.0 ) )
        print( f"exec TestEndpoint, request = {requestSpec}")
        return TestTask( workTime )

    def shutdown(self, **kwargs ): pass

    def capabilities(self, type: str, **kwargs  ) -> Dict:
        if type == "epas":
            return dict( epas = self._epas )

    def init( self ): pass
