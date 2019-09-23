from stratus.app.client import StratusClient
from stratus.app.base import StratusAppBase, StratusFactory
from stratus.app.core import StratusCore
from stratus.app.base import StratusCoreBase
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
import abc


class Handler(StratusFactory):
    __metaclass__ = abc.ABCMeta

    def __init__(self, htype: str, **kwargs):
        StratusFactory.__init__(self, htype, **kwargs)
        self._clients = {}
        self._app: StratusAppBase = None

    @abc.abstractmethod
    def newClient(self, core: StratusCore, **kwargs) -> StratusClient: pass

    def getClient(self, cid = None ) -> Optional[StratusClient]:
        if cid is None:
            return None if len(self._clients) == 0 else list(self._clients.values())[0]
        else: return self._clients.get( cid )

    @abc.abstractmethod
    def newApplication(self, core: StratusCore, **kwargs ) -> StratusAppBase: pass

    def client( self, core: StratusCore, **kwargs ) -> StratusClient:
        cid = kwargs.get("cid")
        activate = kwargs.get( "activate", True )
        client: StratusClient = self.getClient( cid )
        if client is None:
            self.logger.info(f"create client {self.name}:\n kwargs= {kwargs}\n core.parms = {core.parms}\n core.config = {core.config}\n handler.parms = {self.parms}\n handler.type = {self.type}\n handler.name = {self.name}")
            client = self.newClient( core, **self.parms )
            if activate: client.activate()
            self._clients[ client.cid ] = client
        return client

    def app(self, core: StratusCore ) -> StratusAppBase:
        return self.newApplication(core)



