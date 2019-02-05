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

    def init( self ):
        endPointData = self.request( "epas" )
        self.endpointSpecs: List[EndpointSpec] = [ EndpointSpec( epaSpec ) for epaSpec  in endPointData["epas"] ]

    @abc.abstractmethod
    def request(self, epa: str, **kwargs ) -> Dict: pass

    def handles(self, epa: str, **kwargs ) -> bool:
        for endpointSpec in self.endpointSpecs:
            if endpointSpec.handles( epa, **kwargs ): return True
        return False

    def __getitem__( self, key: str ) -> str:
        result =  self.parms.get( key, None )
        assert result is not None, "Missing required parameter in {}: {} ".format( self.__class__.__name__, key )
        return result

    def parm(self, key: str, default: str ) -> str:
        return self.parms.get( key, default  )


