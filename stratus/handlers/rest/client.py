from pyswagger import App
from pyswagger.contrib.client.requests import Client
from pyswagger.spec.v2_0.objects import Operation
from typing import Dict
from ..client import StratusClient

class RestClient(StratusClient):

    def request(self, epp: str, **kwargs) -> Dict:
        op: Operation = self.app.op[ epp ]
        response = self.client.request( op(**kwargs) )
        return response.data

    def init(self):
        self.server = self["server"]
        self.port = self["port"]
        openapi_spec = 'http://{}:{}/{}/swagger.json'.format( self.server, str(self.port), self.api )
        self.app = App._create_( openapi_spec )
        self.client = Client()