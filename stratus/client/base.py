from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView
from flask import Response
import abc

class StratusClient:
    __metaclass__ = abc.ABCMeta

    def __init__( self, api: str, **kwargs ):
        self.api = api
        self.parms = kwargs
        self.init()

    @abc.abstractmethod
    def request(self, api: str, epp: str, req: Dict, **kwargs ) -> Dict: pass

    @abc.abstractmethod
    def handles(self, api: str, epp: str, **kwargs ) -> Dict: pass

    @abc.abstractmethod
    def stat(self, id: str, **kwargs ) -> Dict: pass

    @abc.abstractmethod
    def kill(self, id: str, **kwargs ) -> Dict: pass

    def init( self ) -> Dict: pass

    def __getitem__( self, key: str ) -> str:
        result =  self.parms.get( key, None )
        assert result is not None, "Missing required parameter in {}: {} ".format( self.__class__.__name__, key )
        return result

    def parm(self, key: str, default: str ) -> str:
        return self.parms.get( key, default  )