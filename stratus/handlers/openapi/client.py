from pyswagger import App
from pyswagger.contrib.client.requests import Client
from stratus_endpoint.handler.base import TaskHandle, TaskResult
from pyswagger.spec.v2_0.objects import Operation
from typing import Dict, List
from stratus.app.client import StratusClient, stratusrequest

class OpenApiClient(StratusClient):

    def __init__( self, **kwargs ):
        super(OpenApiClient, self).__init__( "openapi", **kwargs )

    @stratusrequest
    def request(self, requestDict: Dict, inputs: List[TaskResult] = None, **kwargs ) -> TaskHandle:
        op: Operation = self.app.op[ task ]
        response = self.client.request( op(**kwargs) )
        return response.data

    def init(self):
        self.server = self["server"]
        self.port = self["port"]
        self.api = self["api"]
        openapi_spec = 'http://{}:{}/{}/swagger.json'.format( self.server, str(self.port), self.api )
        self.app = App._create_( openapi_spec )
        self.client = Client()
        StratusClient.init( self )
