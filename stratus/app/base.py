import os, json, yaml, abc, itertools
from typing import List, Union, Dict, Set, Iterator
from stratus.util.config import Config, StratusLogger
from app.client import StratusClient
from stratus_endpoint.handler.base import Task
from multiprocessing import Process as SubProcess
from app.operations import ClientOpSet, Op, OpSet

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

    def distributeOps(self, clientOpsets: Dict[str, ClientOpSet]) -> Iterator[ClientOpSet]:
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
        distributed_opsets = [opset.connectedOpsets() for opset in filtered_opsets]
        return itertools.chain.from_iterable(distributed_opsets)

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
        ops = OpSet( ops = [ Op( opDict ) for opDict in ops ] )
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
    from app.core import StratusCore
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
    print( list( distributed_opSets ) )
