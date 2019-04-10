import os
from typing import Dict, List
from stratus_endpoint.handler.test import Endpoint, TaskHandle, TestTask, TaskResult

class TestEndpoint1(Endpoint):

    def __init__( self, **kwargs ):
        Endpoint.__init__( self, **kwargs )
        self._epas = [ f"test{index}" for index in range(10) ]

    def request(self, requestSpec: Dict, inputs: List[TaskResult] = None, **kwargs ) -> "TaskHandle":
        operation = requestSpec["operations"][0]
        workTime = float( operation.get( "workTime", 0.0 ) )
        print( f"exec TestEndpoint, request = {requestSpec}")
        return TestTask( workTime, **self.parms )

    def shutdown(self, **kwargs ): pass

    def capabilities(self, type: str, **kwargs  ) -> Dict:
        if type == "epas":
            return dict( epas = self._epas )

    def init( self ): pass
