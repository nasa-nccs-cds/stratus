from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple
import inspect, stratus.handlers
from stratus.util.domain import UID
import abc, re
import functools
import importlib

class EndpointSpec:
    def __init__(self, epaSpec: str ):
        self._epaSpec = epaSpec

    def handles( self, epa: str, **kwargs ) -> bool:
        return ( re.match( self._epaSpec, epa ) is not None )

    def __str(self):
        return self._epaSpec

class StratusClient:
    __metaclass__ = abc.ABCMeta

    def __init__( self, type: str, name: str, **kwargs ):
        self.type: str = type
        self.name: str = kwargs.get("name")
        self.parms = kwargs
        self.priority: float = float( self.parm( "priority", "0" ) )

    def init( self ):
        endPointData = self.request( "epas" )
        self._endpointSpecs: List[EndpointSpec] = [EndpointSpec(epaSpec) for epaSpec in endPointData["epas"]]

    @abc.abstractmethod
    def request(self, task: str, **kwargs ) -> Dict: pass

    @property
    def endpointSpecs(self) -> List[str]:
        return [str(eps) for eps in self._endpointSpecs]

    def handles(self, epa: str, **kwargs ) -> bool:
        for endpointSpec in self._endpointSpecs:
            if endpointSpec.handles( epa, **kwargs ): return True
        return False

    def __getitem__( self, key: str ) -> str:
        result =  self.parms.get( key, None )
        assert result is not None, "Missing required parameter in {}: {} ".format( self.__class__.__name__, key )
        return result

    def parm(self, key: str, default: str ) -> str:
        return self.parms.get( key, default  )


