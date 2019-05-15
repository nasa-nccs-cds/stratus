from typing import List, Dict, Union
from stratus.app.client import StratusClient
from stratus.handlers.manager import Handlers
from stratus.app.base import StratusAppBase, StratusCoreBase
from stratus.app.operations import Op

class StratusCore(StratusCoreBase):

    def __init__(self, configSpec: Union[str,Dict[str,Dict]], **kwargs ):
        StratusCoreBase.__init__(self, configSpec, **kwargs )
        self.handlers = Handlers( self.config )
        self.service = None

    def getClients( self, op: Op = None, **kwargs ) -> List[StratusClient]:
        return self.handlers.getClients( op )

    def getClient(self) -> StratusClient:
        service = self.handlers.getApplicationHandler()
        assert service is not None, "Can't find [stratus] handler: missing configuration?"
        client = service.client()
        client.activate()
        return client

    def newApplication( self ) -> StratusAppBase:
        service = self.handlers.getApplicationHandler()
        assert service is not None, "Can't find [stratus] handler: missing configuration?"
        app =  service.app(self)
        self.logger.info( "Starting Stratus Node: " + str(app.__class__) )
        return app

    def getEpas(self) -> List[str]:
        return self.handlers.getEpas()
