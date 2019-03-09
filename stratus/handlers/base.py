import string, random, abc, os, yaml, json
from typing import List, Dict, Any, Sequence, Callable, BinaryIO, TextIO, ValuesView, Optional
from stratus.handlers.client import StratusClient
from stratus.handlers.app import StratusAppBase, StratusFactory
from stratus.handlers.core import StratusCore

import abc, sys, pkgutil

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

    def client(self) -> StratusClient:
        if self._client is None:
            self._client = self.newClient()
            self._client.init()
        return self._client

    def app(self, core: StratusCore ) -> StratusAppBase:
        return self.newApplication(core)

