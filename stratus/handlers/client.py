from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple
import inspect, stratus.handlers
import abc, re
import functools
import importlib

class EndpointSpec:
    def __init__(self, epaSpec: str ):
        self._epaSpec = epaSpec

    def handles( self, epa: str, **kwargs ) -> bool:
        return ( re.match( self._epaSpec, epa ) is not None )

class StratusClient:
    __metaclass__ = abc.ABCMeta

    def __init__( self, type: str, **kwargs ):
        self.type = type
        self.parms = kwargs
        self.init()
        self.endpointSpecs: List[EndpointSpec] = [ EndpointSpec( epaSpec ) for epaSpec  in self.request( "epas" )["epas"] ]

    @abc.abstractmethod
    def request(self, epa: str, **kwargs ) -> Dict: pass

    @abc.abstractmethod
    def handles(self, epa: str, **kwargs ) -> bool:
        for endpointSpec in self.endpointSpecs:
            if endpointSpec.handles( epa, **kwargs ): return True
        return False

    @abc.abstractmethod
    def init( self ): pass

    def __getitem__( self, key: str ) -> str:
        result =  self.parms.get( key, None )
        assert result is not None, "Missing required parameter in {}: {} ".format( self.__class__.__name__, key )
        return result

    def parm(self, key: str, default: str ) -> str:
        return self.parms.get( key, default  )


class ClientFactory:

    def getClient( type: str, **kwargs ) -> StratusClient:
        if type == "openapi": pass
        return None


if __name__ == "__main__":
    members = inspect.getmembers(stratus.handlers, inspect.ismodule)
    module = importlib.import_module("stratus.handlers")
    print( "." )
