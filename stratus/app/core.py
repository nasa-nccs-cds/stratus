from typing import List, Dict, Union
from app.client import StratusClient
from stratus.handlers.manager import Handlers
from app.base import StratusAppBase, StratusCoreBase

class StratusCore(StratusCoreBase):

    def __init__(self, configSpec: Union[str,Dict[str,Dict]], **kwargs ):
        StratusCoreBase.__init__(self, configSpec, **kwargs )
        self.handlers = Handlers( self.config )
        self.service = None

    def getClients( self, epas: List[str] = None ) -> List[StratusClient]:
        return self.handlers.getClients( epas )

    def getClient(self) -> StratusClient:
        service = self.handlers.getApplicationHandler()
        assert service is not None, "Can't find [stratus] handler: missing configuration?"
        client = service.client()
        client.activate()
        return client

    def getApplication( self ) -> StratusAppBase:
        service = self.handlers.getApplicationHandler()
        assert service is not None, "Can't find [stratus] handler: missing configuration?"
        app =  service.app(self)
        self.logger.info( "Starting Stratus Node: " + str(app.__class__) )
        return app

    def getEpas(self) -> List[str]:
        return self.handlers.getEpas()
