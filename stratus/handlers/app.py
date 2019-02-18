import os, json
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional, Set, Tuple
from stratus.util.config import Config, StratusLogger, UID
from stratus.handlers.client import StratusClient
from stratus.handlers.manager import handlers

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

    def submit( self, request: Dict ) -> Dict:
        filtered_request =  self.getFilteredRequest(request)
        self.logger.info( "Client {}: submit operations {}".format( self.client.name, str( filtered_request['operations'] ) ) )
        return self.client.request( "exe", **filtered_request )

class StratusCore:
    HERE = os.path.dirname(__file__)
    SETTINGS = os.path.join( HERE, 'settings.ini')

    def __init__(self, **kwargs ):
        self.logger = StratusLogger.getLogger()
        settings = kwargs.get( "settings", os.environ.get( 'STRATUS_SETTINGS', self.SETTINGS ) )
        self.config = Config(settings)
        self.parms = self.getConfigParms('stratus')
        handlersSpec = self["HANDLERS"]
        handlers.init( handlersSpec )

    def getConfigParms(self, module: str ) -> Dict:
        return self.config.get_map( module )

    @classmethod
    def getClients( cls, epa: str ) -> List[StratusClient]:
        from stratus.handlers.manager import handlers
        return handlers.getClients( epa )

    @classmethod
    def geClientOpsets(cls, request: Dict ) -> Dict[str,OpSet]:
        # Returns map of client id to list of ops in request that can be handled by that client
        ops = request.get("operations")
        clientOpsets: Dict[str,OpSet] = dict()
        for op in ops:
            for parm in [ "epa", "id"]:
                assert parm in op, "Operation must have an '{}' parameter: {}".format( parm, str(op) )
            epa = op["epa"]
            clients = StratusCore.getClients( epa )
            for client in clients:
               opSet = clientOpsets.setdefault( client.name, OpSet(client) )
               opSet.add( op )
        return clientOpsets

    @classmethod
    def distributeOps(cls,  clientOpsets: Dict[str, OpSet] ) -> List[OpSet]:
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

    @classmethod
    def processWorkflow(cls, request: Dict ) -> Dict[str,OpSet]:
        clientOpsets: Dict[str, OpSet] = cls.geClientOpsets(request)
        distributed_opSets = cls.distributeOps( clientOpsets )
        responses = { opset.name: opset.submit( request ) for opset in distributed_opSets }
        return responses

    def parm(self, name: str, default = None ) -> str:
        parm = self.parms.get( name, default )
        if parm is None: raise Exception( "Missing required stratus parameter in settings.ini: " + name )
        return parm

    def __getitem__( self, key: str ) -> str:
        result =  self.parms.get( key, None )
        assert result is not None, "Missing required parameter in {}: {} ".format( self.__class__.__name__, key )
        return result

if __name__ == "__main__":
#        sCore = StratusCore()
#        request = { "operations": [ {"epa": "1"}, {"epa": "2"}, {"epa": "3"} ]}
#        print( sCore.processWorkflow( request ) )

    clientMap = dict()
    opSet2 = clientMap.setdefault( "t2", OpSet("t2") )
    opSet2.add( "op2"  )
    opSet2.add( "op3" )


    opSet1 = clientMap.setdefault( "t1", OpSet("t1") )
    opSet1.add( "op1"  )


    for value in sorted(clientMap.values(),reverse=True):
        print( str(value)  )


