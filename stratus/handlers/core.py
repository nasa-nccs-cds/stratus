import os, json, yaml
from typing import List, Dict, Union, Sequence, BinaryIO, TextIO, ValuesView, Optional, Set, Tuple
from stratus.util.config import Config, StratusLogger, UID
from stratus.handlers.client import StratusClient
from stratus.handlers.manager import Handlers
from stratus.handlers.app import StratusAppBase, StratusCoreBase

class StratusCore(StratusCoreBase):

    def __init__(self, configSpec: Union[str,Dict[str,Dict]], **kwargs ):
        StratusCoreBase.__init__(self, configSpec, **kwargs )
        self.handlers = Handlers( self.config )
        self.service = None

    def getClients( self, epa: str = None ) -> List[StratusClient]:
        return self.handlers.getClients( epa )

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
