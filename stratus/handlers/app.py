import os, json
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional, Set, Tuple
from stratus.util.config import Config, StratusLogger
from stratus.handlers.client import StratusClient
from stratus.util.domain import UID

class OpSet():

    def __init__( self, client: StratusClient ):
        self.ops: Set[Dict] = set()
        self.client: StratusClient = client

    def __iter__(self):
        return self.ops.__iter__()

    def add(self, op: Dict):
        self.ops.add( op )

    @property
    def name(self):
        return self.client.name

    def remove(self, op: Dict):
        self.ops.remove( op )

    def __len__(self):
        return self.ops.__len__()

    def __eq__(self, other: "OpSet"):
        return len( self.ops ) == len( other )

    def __lt__(self, other: "OpSet"):
        return len( self.ops ) < len( other )

    def __str__(self):
        return "C({}):[{}]".format( self.client, ",".join( [ str(op) for op in self.ops ] ) )

    def getFilteredRequest(self, request: Dict ) -> str:
        filtered_request = dict( request )
        filtered_request["operations"] = list( self.ops )
        return json.dumps( filtered_request )

    def submit( self, request: Dict ) -> Dict:
        return self.client.request( self.getFilteredRequest(request) )

class StratusCore:
    HERE = os.path.dirname(__file__)
    SETTINGS = os.path.join( HERE, 'settings.ini')

    def __init__(self):
        self.logger = StratusLogger.getLogger()
        settings = os.environ.get( 'STRATUS_SETTINGS', self.SETTINGS )
        self.config = Config(settings)

    def getConfigParms(self, module: str ) -> Dict:
        return self.config.get_map( module )

    @classmethod
    def getClients( cls, epa: str ) -> List[StratusClient]:
        from stratus.handlers.manager import handlers
        return handlers.getClients( epa )

    def processWorkflow(self, request: Dict ) -> Dict[str,Dict]:
        ops = request.get("operations")
        clientMap: Dict[str,OpSet] = dict()
        for op in ops:
            epa = op["epa"]
            clients = StratusCore.getClients( epa )
            for client in clients:
               opSet = clientMap.setdefault( client.name, OpSet(client) )
               opSet.add( op )

        filtered_opsets: List[OpSet] = []
        processed_ops: List[OpSet] = []
        for opset in sorted( clientMap.values(), reverse=True ):
            for op in opset:
                if op["id"] in processed_ops: opset.remove(op)
                else: processed_ops.append( op["id"] )
            if len( opset ) > 0: filtered_opsets.append( opset )

        responses = { opset.name: opset.submit( request ) for opset in filtered_opsets }

        return responses





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


