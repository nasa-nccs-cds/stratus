from app.client import StratusClient
from app.base import StratusAppBase, StratusFactory
from app.core import StratusCore

import abc


class Handler(StratusFactory):
    __metaclass__ = abc.ABCMeta

    def __init__(self, htype: str, **kwargs):
        StratusFactory.__init__(self, htype, **kwargs)
        self._client = None
        self._app: StratusAppBase = None

    @abc.abstractmethod
    def newClient(self) -> StratusClient: pass

    @abc.abstractmethod
    def newApplication(self, core: StratusCore ) -> StratusAppBase: pass

    def client( self ) -> StratusClient:
        if self._client is None:
            self._client = self.newClient()
            self._client.activate()
        return self._client

    def app(self, core: StratusCore ) -> StratusAppBase:
        return self.newApplication(core)

