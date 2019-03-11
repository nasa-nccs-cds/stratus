from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from stratus.util.config import Config, StratusLogger, UID
from stratus_endpoint.handler.base import Task, Status
import abc, re
import functools
import importlib

class EndpointSpec:
    def __init__(self, epaSpec: str ):
        self.logger = StratusLogger.getLogger()
        self._epaSpec = epaSpec

    def handles( self, epa: str, **kwargs ) -> bool:
        try:
            return ( re.match( self._epaSpec, epa ) is not None )
        except Exception as err:
            self.logger.error( f"Error Checking EPA '{epa}' against epaSpec '{self._epaSpec}': {str(err)}")
            return False

    def __str__(self):
        return self._epaSpec

def stratusrequest( requestMethod ):
    @functools.wraps(requestMethod)
    def udpatedRequestMethod( self, requestSpec: Dict, **kwargs  ):
        requestDict =  self.updateMetadata( requestSpec )
        return requestMethod( self, requestDict, **kwargs )
    return udpatedRequestMethod

class StratusClient:
    __metaclass__ = abc.ABCMeta
    logger = StratusLogger.getLogger()

    def __init__( self, type: str, **kwargs ):
        self.cid = UID.randomId(6)
        self.type: str = type
        self.name: str = kwargs.get("name")
        self.parms = kwargs
        self.priority: float = float( self.parm( "priority", "0" ) )
        self.active = False

    def init( self ):
        endPointData = self.capabilities("epas")
        if "error" in endPointData: raise Exception( "Error accessing endpoint data: " + endPointData["message"] )
        self.logger.info( "EndpointSpecs: " + str(endPointData))
        self._endpointSpecs: List[EndpointSpec] = [ EndpointSpec(epaSpec) for epaSpec in endPointData["epas"] ]
        self.active = True

    @abc.abstractmethod
    @stratusrequest
    def request(self, request: Dict, **kwargs ) -> Task: pass

    @abc.abstractmethod
    def status(self, **kwargs ) -> Status: pass

    @abc.abstractmethod
    def capabilities(self, type: str, **kwargs ) -> Dict: pass

    @property
    def endpointSpecs(self) -> List[str]:
        return [str(eps) for eps in self._endpointSpecs]

    def handles(self, epa: str, **kwargs ) -> bool:
        for endpointSpec in self._endpointSpecs:
            if endpointSpec.handles( epa, **kwargs ): return True
        return False

    def __getitem__( self, key: str ) -> str:
        result =  self.parms.get( key, None )
        assert result is not None, "Missing required parameter in {}: {}, params: {} ".format( self.__class__.__name__, key, str(self.parms) )
        return result

    def parm(self, key: str, default: str = None) -> Optional[str]:
        return self.parms.get( key, default  )

    def updateMetadata(self, requestSpec: Dict ) -> Dict:
        requestDict = dict(requestSpec)
        requestDict["rid"] = requestSpec.get("rid",UID.randomId(6))
        requestDict["cid"] = self.cid
        return requestDict



