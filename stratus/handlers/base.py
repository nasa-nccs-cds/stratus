import string, random, abc, os, yaml, json
from typing import List, Dict, Any, Sequence, Callable, BinaryIO, TextIO, ValuesView, Optional
from stratus.handlers.client import StratusClient, TestClient
from stratus.handlers.app import StratusAppBase, StratusCore

import abc, sys, pkgutil

class Handler:
    __metaclass__ = abc.ABCMeta

    def __init__(self, htype: str, **kwargs):
        self.parms = kwargs
        self.name = self['name']
        self.type: str = htype
        self._client = None
        self._app: StratusAppBase = None
        htype1 = self.parms.pop("type")
        assert htype1 == htype, "Sanity check of Handler type failed: {} vs {}".format(htype1,htype)

    def __getitem__( self, key: str ) -> str:
        result =  self.parms.get( key, None )
        assert result is not None, "Missing required parameter in {}: {} ".format( self.__class__.__name__, key )
        return result

    def parm(self, key: str, default: str ) -> str:
        return self.parms.get( key, default  )

    @abc.abstractmethod
    def newClient(self) -> StratusClient: pass

    @abc.abstractmethod
    def newApplication(self, core: StratusCore ) -> StratusAppBase: pass

    @property
    def client(self) -> StratusClient:
        if self._client is None:
            self._client = self.newClient()
            self._client.init()
        return self._client

    def app(self, core: StratusCore ) -> StratusAppBase:
        return self.newApplication(core)

    def __repr__(self):
        return json.dumps( self.parms )

class TestHandler(Handler):

    def __init__(self, **kwargs):
        Handler.__init__(self,"test",**kwargs)

    def newClient(self) -> StratusClient:
        return TestClient(**self.parms)

    def newApplication(self, core: StratusCore ) -> StratusAppBase:
        return StratusAppBase(core)
