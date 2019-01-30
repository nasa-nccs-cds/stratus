from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple
import abc, re

class EndpointSpec:
    def __init__(self, epaSpec: str, apiSpec: str ):
        self._epaSpec = epaSpec
        self._apiSpec = apiSpec

    def handles( self, api: str, epa: str, **kwargs ) -> bool:
        return ( re.match( self._epaSpec, epa ) is not None ) and ( re.match( self._apiSpec, api ) is not None )

class StratusClient:
    __metaclass__ = abc.ABCMeta

    def __init__( self, type: str, **kwargs ):
        self.type = type
        self.parms = kwargs
        self.endpointSpecs: List[EndpointSpec] = [ EndpointSpec( epaSpec, apiSpec ) for (epaSpec,apiSpec)  in self.getEndpointSpecs( ) ]
        self.init()

    @abc.abstractmethod
    def request(self, api: str, req: Dict, **kwargs ) -> Dict: pass

    @abc.abstractmethod
    def handles(self, api: str, epa: str, **kwargs ) -> bool:
        for endpointSpec in self.endpointSpecs:
            if endpointSpec.handles( api, epa, **kwargs ): return True
        return False

    @abc.abstractmethod
    def getEndpointSpecs(self, **kwargs ) -> List[Tuple[str,str]]: pass

    @abc.abstractmethod
    def stat(self, id: str, **kwargs ) -> Dict: pass

    @abc.abstractmethod
    def health(self, id: str, **kwargs ) -> Dict: pass

    @abc.abstractmethod
    def kill(self, id: str, **kwargs ) -> Dict: pass

    def init( self ) -> Dict: pass

    def __getitem__( self, key: str ) -> str:
        result =  self.parms.get( key, None )
        assert result is not None, "Missing required parameter in {}: {} ".format( self.__class__.__name__, key )
        return result

    def parm(self, key: str, default: str ) -> str:
        return self.parms.get( key, default  )


class ClientFactory:

    def getClient( type: str, **kwargs ) -> StratusClient:
        pass