import os, json, yaml, abc
from typing import List, Union, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional, Set, Tuple, Iterator, Iterable
from stratus.util.config import Config, StratusLogger, UID
from stratus.handlers.client import StratusClient
from enum import Enum
from stratus_endpoint.handler.base import Task, Status
from multiprocessing import Process as SubProcess
import sys

class Op:
    def __init__( self, params: Dict = None ):
        self.params: Dict = params if params else {}
        self.id = self.get( "id", UID.randomId( 6 ) )
        name_toks = self.get("name").split(":")
        self.name: str = name_toks[-1]
        self.epas: List[str]  = name_toks[:-1]
        input_parm = self.get("input")
        self.inputs: List[str] = input_parm.split(",") if isinstance( input_parm, str ) else input_parm
        self.result = self.get( "result", UID.randomId( 6 ) )
        self.dependencies: Dict[str,"Op"] = {}

    def get(self, name: str, default = None ) -> str:
        parm = self.params.get( name, default )
        if parm is None: raise Exception( "Missing required parameter in operation: " + name )
        return parm

    def __getitem__( self, key: str ) -> str:
        result =  self.params.get( key, None )
        assert result is not None, f"Missing required parameter in operation: {key} "
        return result

    def addDependency(self, op: "Op"):
        self.dependencies[op.id] = op

class OpSet():

    def __init__( self, ops: Iterable[Op] ):
        self.logger = StratusLogger.getLogger()
        self.ops: Dict[str,Op] = { op.id: op for op in ops }

    def __iter__(self) -> Iterator[Op]:
        return self.ops.values().__iter__()

    def __hash__(self):
        return hash(repr(self))

    def add(self, op: Op):
        self.ops[op.id] = op

    def __repr__(self):
        keys = list(self.ops.keys())
        keys.sort()
        return "-".join(keys)

    def linkDependencies(self):
        for op in self.ops.values():
            for vid in op.inputs:
                for op1 in self.ops.values():
                    if op1.result ==  vid:
                        op.addDependency( op1 )

    def remove(self, opIds: List[str]):
        for oid in opIds:
            try: del self.ops[ oid ]
            except: pass

    def __len__(self):
        return self.ops.__len__()

    def __eq__(self, other: "ClientOpSet"):
        return len( self.ops ) == len( other )

    def __lt__(self, other: "ClientOpSet"):
        return len( self.ops ) < len( other )

    def getFilteredRequest(self, request: Dict ) -> Dict:
        operations = []
        filtered_request = {}
        for op in self.ops.values():
            operations.append( op.params )
            for epa in op.epas:
                for key,value in request.items():
                    parm_epas = key.split(":")[:-1]
                    if (len(parm_epas) == 0) or (epa in parm_epas):
                        filtered_request[key] = value
        filtered_request["operations"] = operations
        return filtered_request

class ClientOpSet(OpSet):

    def __init__( self, client: StratusClient, **kwargs ):
        OpSet.__init__( self, kwargs.get( "ops", [] ) )
        self.client: StratusClient = client

    def new(self) -> "ClientOpSet":
        return ClientOpSet(self.client)

    @property
    def name(self) -> str:
        return self.client.name

    def __str__(self):
        return "C({}):[{}]".format( self.client, ",".join( [ op for op in self.ops.keys() ] ) )

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

    def getCapabilities(self, ctype ) -> Dict:
        caps = {}
        for client in self.getClients():
            caps.update( client.capabilities(ctype) )
        return caps

    @abc.abstractmethod
    def getClients( self, epas: List[str] = None ) -> List[StratusClient]: pass

class StratusAppBase:
    __metaclass__ = abc.ABCMeta

    def __init__( self, _core: StratusCoreBase ):
        self.logger = StratusLogger.getLogger()
        self.core = _core

    def distributeOps(self, clientOpsets: Dict[str, ClientOpSet]) -> Set[ClientOpSet]:
        # Distributes ops to clients while maximizing locality of operations
        filtered_opsets: Set[ClientOpSet] = set()
        processed_ops: List[str] = []
        sorted_opsets: List[ClientOpSet] = list(sorted(clientOpsets.items(), reverse=True, key=lambda x: x[1]))
        while len( sorted_opsets ):
            cid, base_opset = sorted_opsets.pop(0)
            new_opset = base_opset.new()
            for op in base_opset:
                if op.id not in processed_ops:
                    processed_ops.append( op.id )
                    new_opset.add( op )
            if len( new_opset ) > 0:
                filtered_opsets.add( new_opset )
            for cid, opset in sorted_opsets:
                opset.remove( processed_ops )
            sorted_opsets = list( sorted( sorted_opsets, reverse=True, key=lambda x: x[1] ) )
        return filtered_opsets

    def processWorkflow( self, request: Dict ) -> Dict[str,Task]:
        clientOpsets: Dict[str, ClientOpSet] = self.geClientOpsets(request)
        distributed_opSets = self.distributeOps( clientOpsets )
        responses = { opset.name: opset.submit( request ) for opset in distributed_opSets }
        return responses

    def geClientOpsets(self, request: Dict ) -> Dict[str, ClientOpSet]:
        # Returns map of client id to list of ops in request that can be handled by that client
        ops = request.get("operation")
        assert ops is not None, "Missing 'operation' parameter in request: " + str( request )
        clientOpsets: Dict[str, ClientOpSet] = dict()
        ops = OpSet( [ Op( opDict ) for opDict in ops ] )
        ops.linkDependencies()
        for op in ops:
            clients = self.core.getClients( op.epas )
            assert len(clients) > 0, f"Can't find a client to process the operation': {op.epas}"
            for client in clients:
               opSet = clientOpsets.setdefault(client.name, ClientOpSet(client))
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
    def run(self): pass

    def exec(self) -> SubProcess:
        proc = SubProcess( target=self.run )
        proc.start()
        return proc

class StratusFactory:
    __metaclass__ = abc.ABCMeta

    def __init__(self, htype: str, **kwargs):
        self.parms = kwargs
        self.name = self['name']
        self.type: str = htype
        htype1 = self.parms.pop("type")
        assert htype1 == htype, "Sanity check of Handler type failed: {} vs {}".format(htype1,htype)

    @abc.abstractmethod
    def client( self ) -> StratusClient: pass

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


if __name__ == "__main__":
    from stratus.handlers.core import StratusCore
    from stratus_endpoint.handler.base import Task

    settings = dict( stratus=dict( type="zeromq"), edas=dict(type="test", work_time=2.0 ) )
    core = StratusCore(settings)
    app = core.getApplication()

    request = {    "edas:domain": [ { "name": "d0", "time": {"start": "1980-01-01", "end": "2001-12-31", "crs": "timestamps"} } ],
                    "edas:input":  [ { "uri": "collection:merra2", "name": "tas:v1", "domain": "d1" } ],
                    "operation":   [ { "name": "test:ave",  "input": "v1", "axis": "yt", "result": "v1ave" },
                                     { "name": "test:diff", "input": ["v1", "v1ave"] } ] }

    clientOpsets: Dict[str, ClientOpSet] = app.geClientOpsets(request)
    distributed_opSets = app.distributeOps(clientOpsets)
