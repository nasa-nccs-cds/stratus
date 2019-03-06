import os, json, yaml, abc
from typing import List, Union, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional, Set, Tuple
from stratus.util.config import Config, StratusLogger, UID
from stratus.handlers.client import StratusClient
from enum import Enum
from stratus_endpoint.handler.base import Task, Status

class ExecMode(Enum):
    INLINE = 0
    THREAD = 1
    SUBPROCESS = 2

class OpSet():

    def __init__( self, client: StratusClient ):
        self.logger = StratusLogger.getLogger()
        self.ops: Dict[str,Dict] = {}
        self.client: StratusClient = client

    def __iter__(self):
        return self.ops.values().__iter__()

    def add(self, op: Dict):
        self.ops[ op["id"] ] = op

    def new(self):
        return OpSet(self.client)

    @property
    def name(self):
        return self.client.name

    def remove(self, opIds: List[str]):
        for oid in opIds:
            try: del self.ops[ oid ]
            except: pass

    def __len__(self):
        return self.ops.__len__()

    def __eq__(self, other: "OpSet"):
        return len( self.ops ) == len( other )

    def __lt__(self, other: "OpSet"):
        return len( self.ops ) < len( other )

    def __str__(self):
        return "C({}):[{}]".format( self.client, ",".join( [ op for op in self.ops.keys() ] ) )

    def getFilteredRequest(self, request: Dict ) -> Dict:
        filtered_request = dict( request )
        filtered_request["operations"] = list( self.ops.values() )
        return filtered_request

    def submit( self, request: Dict ) -> Task:
        filtered_request =  self.getFilteredRequest(request)
        self.logger.info( "Client {}: submit operations {}".format( self.client.name, str( filtered_request['operations'] ) ) )
        return self.client.request( filtered_request )

class StratusCoreBase:
    HERE = os.path.dirname(__file__)
    SETTINGS = os.path.join( HERE, 'settings.ini')

    def __init__(self, configSpec: Union[str,Dict[str,Dict]], **kwargs ):
        self.logger = StratusLogger.getLogger()
        self.config = self.getSettings( configSpec )
        self.parms = self.getConfigParms('stratus')

    @classmethod
    def getSettings( cls, configSpec: Union[str,Dict[str,Dict]] ) -> Dict[str,Dict]:
        result = {}
        if isinstance(configSpec, str ):
            assert os.path.isfile(configSpec), "Settings file does not exist: " + configSpec
            if configSpec.endswith( ".ini" ):
                config =  Config(configSpec)
                for section in config.sections():
                    result[section] = config.get_map( section )
            elif ( configSpec.endswith( ".yml" ) or configSpec.endswith( ".yaml" ) ):
                with open(configSpec, 'r') as stream:
                    result =  yaml.load(stream)
        else:
            result = configSpec
        return result

    def getConfigParms(self, module: str ) -> Dict:
        return self.config.get( module, {} )

    def parm(self, name: str, default = None ) -> str:
        parm = self.parms.get( name, default )
        if parm is None: raise Exception( "Missing required stratus parameter in settings.ini: " + name )
        return parm

    def __getitem__( self, key: str ) -> str:
        result =  self.parms.get( key, None )
        assert result is not None, "Missing required parameter in {}: {} ".format( self.__class__.__name__, key )
        return result

    @abc.abstractmethod
    def getClients( self, epa ) -> List[StratusClient]: pass

class StratusAppBase:
    __metaclass__ = abc.ABCMeta

    def __init__( self, _core: StratusCoreBase ):
        self.logger = StratusLogger.getLogger()
        self.core = _core

    def distributeOps(self,  clientOpsets: Dict[str, OpSet] ) -> List[OpSet]:
        # Distributes ops to clients while maximizing locality of operations
        filtered_opsets: List[OpSet] = []
        processed_ops: List[str] = []
        sorted_opsets: List[OpSet] = list( sorted( clientOpsets.items(), reverse=True, key=lambda x: x[1] ) )
        while len( sorted_opsets ):
            cid, base_opset = sorted_opsets.pop(0)
            new_opset = base_opset.new()
            for op in base_opset:
                if op["id"] not in processed_ops:
                    processed_ops.append( op["id"] )
                    new_opset.add( op )
                if len( new_opset ) > 0: filtered_opsets.append( new_opset )
            for cid, opset in sorted_opsets:
                opset.remove( processed_ops )
            sorted_opsets = list( sorted( sorted_opsets, reverse=True, key=lambda x: x[1] ) )
        return filtered_opsets

    def processWorkflow( self, request: Dict ) -> Dict[str,Task]:
        clientOpsets: Dict[str, OpSet] = self.geClientOpsets(request)
        distributed_opSets = self.distributeOps( clientOpsets )
        responses = { opset.name: opset.submit( request ) for opset in distributed_opSets }
        return responses

    def geClientOpsets(self, request: Dict ) -> Dict[str,OpSet]:
        # Returns map of client id to list of ops in request that can be handled by that client
        ops = request.get("operation")
        assert ops is not None, "Missing 'operation' parameter in request: " + str( request )
        clientOpsets: Dict[str,OpSet] = dict()
        for op in ops:
            if not "id" in op: op["id"] = UID.randomId( 6 )
            for parm in [ "epa" ]:
                assert parm in op, "Operation must have an '{}' parameter: {}".format( parm, str(op) )
            epa = op["epa"]
            clients = self.core.getClients( epa )
            for client in clients:
               opSet = clientOpsets.setdefault( client.name, OpSet(client) )
               opSet.add( op )
        return clientOpsets

    def shutdown(self): pass

    def parm(self, name: str, default = None ) -> str:
        return self.core.parm( name, default )

    def __getitem__( self, key: str ) -> str:
        return self.core[key]

    def getConfigParms(self, module: str ) -> Dict:
        return self.core.getConfigParms( module )

    @abc.abstractmethod
    def run(self, execMode: ExecMode = ExecMode.INLINE ): pass

class StratusFactory:
    __metaclass__ = abc.ABCMeta

    def __init__(self, htype: str, **kwargs):
        self.parms = kwargs
        self.name = self['name']
        self.type: str = htype
        htype1 = self.parms.pop("type")
        assert htype1 == htype, "Sanity check of Handler type failed: {} vs {}".format(htype1,htype)

    @abc.abstractmethod
    def client(self) -> StratusClient: pass

    @abc.abstractmethod
    def app(self, core: StratusCoreBase ) -> StratusAppBase: pass

    def __getitem__( self, key: str ) -> str:
        result =  self.parms.get( key, None )
        assert result is not None, "Missing required parameter in {}: {} ".format( self.__class__.__name__, key )
        return result

    def parm(self, key: str, default: str) -> str:
        return self.parms.get(key, default)

    def __repr__(self):
        return json.dumps(self.parms)
