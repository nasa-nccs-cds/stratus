from stratus.handlers.client import StratusClient
from stratus_endpoint.handler.base import Task, Status, Endpoint
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple
import importlib, traceback

class DirectClient(StratusClient):

    def __init__( self, **kwargs ):
        super(DirectClient, self).__init__( "endpoint", **kwargs )
        self.endpoint: Endpoint = None

    def instantiateEndpoint( self ):
        module = self["module"]
        class_name = self["class"]
        module = importlib.import_module(module)
        epclass = getattr(module, class_name)
        return epclass( **self.parms )

    def request(self, requestSpec: Dict, **kwargs ) -> Task:
        return self.endpoint.request( requestSpec, **kwargs )

    def capabilities(self, type: str, **kwargs ) -> Dict:
        return self.endpoint.capabilities( type, **kwargs )

    def init(self, **kwargs):
        try:
            self.endpoint: Endpoint = self.instantiateEndpoint()
            self.endpoint.init()
            super(DirectClient, self).init()

        except Exception as err:
            err_msg =  "\n-------------------------------\nWorker Init error: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() )
            self.logger.error(err_msg)
            if self.endpoint is not None:
                self.endpoint.shutdown()
