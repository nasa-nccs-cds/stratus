from app.client import StratusClient
from app.base import StratusAppBase, StratusFactory
from app.core import StratusCore
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
import abc


class Handler(StratusFactory):
    __metaclass__ = abc.ABCMeta

    def __init__(self, htype: str, **kwargs):
        StratusFactory.__init__(self, htype, **kwargs)
        self._clients = {}
        self._app: StratusAppBase = None

    @abc.abstractmethod
    def newClient( self, cid = None, gateway=False ) -> StratusClient: pass

    def getClient(self, cid = None ) -> Optional[StratusClient]:
        if cid is None:
            return None if len(self._clients) == 0 else list(self._clients.values())[0]
        else: return self._clients.get( cid )

    @abc.abstractmethod
    def newApplication(self, core: StratusCore ) -> StratusAppBase: pass

    def client( self, cid = None ) -> StratusClient:
        client = self.getClient( cid )
        if client is None:
            client = self.newClient(cid)
            client.activate()
            self._clients[ client.cid ] = client
        return client

    def app(self, core: StratusCore ) -> StratusAppBase:
        return self.newApplication(core)

