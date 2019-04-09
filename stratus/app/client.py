from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from stratus.util.config import Config, StratusLogger, UID
from stratus_endpoint.handler.base import TaskHandle, Status, TaskResult
import abc, re
from decorator import decorator, dispatch_on

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

@decorator
def stratusrequest( func, *args, **kwargs ):
    args[0].updateMetadata( args[1] )
    return func( *args, **kwargs)

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
        self._endpointSpecs: List[EndpointSpec] = None
        self.clients = { self.cid }

    def activate(self):
        self.active = True

    def init( self ):
        if self._endpointSpecs is None:
            endPointData = self.capabilities("epas")
            if "error" in endPointData: raise Exception( "Error accessing endpoint data: " + endPointData["message"] )
            self.logger.info( "EndpointSpecs: " + str(endPointData))
            self._endpointSpecs = [ EndpointSpec(epaSpec) for epaSpec in endPointData["epas"] ]
            self.activate()

    @abc.abstractmethod
    @stratusrequest
    def request(self, request: Dict, inputs: List[TaskResult] = None, **kwargs ) -> TaskHandle: pass

    @abc.abstractmethod
    def status(self, **kwargs ) -> Status: pass

    @abc.abstractmethod
    def capabilities(self, type: str, **kwargs ) -> Dict: pass

    @property
    def endpointSpecs(self) -> List[str]:
        self.init()
        return [str(eps) for eps in self._endpointSpecs]

    def handles(self, epa: str, **kwargs ) -> bool:
        self.init()
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
        source_client = requestDict.get("cid")
        requestDict["rid"] = requestSpec.get("rid",UID.randomId(6))
        if source_client: self.clients.add( source_client )
        requestDict["cid"] = self.cid
        requestDict["clients"] = ",".join( self.clients )
        return requestDict

    def hasClient(self, cid: str ) -> bool:
        return cid in self.clients



