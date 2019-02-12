import os
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
from stratus.util.config import Config, StratusLogger
from stratus.handlers.client import StratusClient
from sortedcontainers import SortedDict

class OpSet:

    def __init__(self):
        self.ops = set()

    def add(self, op):
        self.ops.add( op )

    def __len__(self):
        return self.ops.__len__()

    def __eq__(self, other):
        return len( self.ops ) == len( other )

    def __lt__(self, other):
        return len( self.ops ) < len( other )

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

    def processWorkflow(self, request: Dict ):
        ops = request.get("operations")
        clientMap = SortedDict()
        for op in ops:
            epa = op["epa"]
            clients = StratusCore.getClients( epa )
            for client in clients:
               opSet = clientMap.setdefault( client, OpSet() )
               opSet.add( op )
        return clientMap


if __name__ == "__main__":
        sCore = StratusCore()
        request = { "operations": [ {"epa": "1"}, {"epa": "2"}, {"epa": "3"} ]}
        print( sCore.processWorkflow( request ) )




