import copy
from typing import List, Dict, Set, Iterator
from stratus.util.config import StratusLogger, UID
from app.client import StratusClient
from decorator import decorator
from stratus_endpoint.handler.base import Task
from stratus.app.graph import DGNode
import networkx as nx



class Op(DGNode):

    def __init__( self, **kwargs ):
        DGNode. __init__( self, **kwargs )
        name_toks = self.get("name").split(":")
        self.name: str = name_toks[-1]
        self.epas: List[str]  = name_toks[:-1]
        input_parm = self.get("input")
        self._inputs: List[str] = input_parm.split(",") if isinstance( input_parm, str ) else input_parm
        self._result = self.get( "result", UID.randomId( 6 ) )

    def inputs(self)-> List[str]: return self._inputs
    def outputs(self)-> List[str]: return [ self._result ]

class OpSet():

    def __init__( self, **kwargs ):
        self.logger = StratusLogger.getLogger()
        self.ops: Dict[str,Op] = {}
        self.graph = kwargs.get( "graph", nx.DiGraph( ) )
        for op in kwargs.get( "ops", [] ): self.add( op )

    def addDependency(self, src: str, dest: str ):
        self.graph.add_edge( src, dest )
        edges = self.graph.edges
        return edges

    def __iter__(self) -> Iterator[Op]:
        return self.ops.values().__iter__()

    def __hash__(self):
        return hash(repr(self))

    def ids(self) -> Set[str]:
        return set( self.ops.keys() )

    def add(self, op: Op):
        self.ops[op.id] = op
        self.graph.add_node(op.id)

    def __repr__(self):
        keys = list(self.ops.keys())
        keys.sort()
        return "-".join(keys)

    def connect(self):
        for op in self.ops.values():
            for vid in op.inputs:
                for op1 in self.ops.values():
                    if op1.result ==  vid:
                        self.addDependency( op.id, op1.id )

    @graphop
    def connectedComponents(self):
        components = nx.weakly_connected_components(self.graph)
        return [subgraph_iops for subgraph_iops in components]

    def inputs(self):
        pass

    def remove(self, opIds: List[str]):
        for oid in opIds:
            try:
                del self.ops[ oid ]
                self.graph.remove_node( oid )
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
        OpSet.__init__( self,  **kwargs )
        self.client: StratusClient = client

    def connectedOpsets(self) -> List["ClientOpSet"]:
        subgraphs = self.connectedComponents()
        return [ self.filter( subgraph_iops ) for subgraph_iops in subgraphs ]

    def copy(self) -> "ClientOpSet":
        return ClientOpSet( self.client, ops=self.ops.values(), graph = copy.deepcopy(self.graph) )

    def new(self) -> "ClientOpSet":
        return ClientOpSet(self.client)

    def filter(self, iops: Set[str] ) -> "ClientOpSet":
        filteredOpset = self.copy()
        filteredOpset.remove( list( self.ids() ^  iops ) )
        return filteredOpset

    @property
    def name(self) -> str:
        return self.client.name

    def __str__(self):
        return "C({}):[{}]".format( self.client, ",".join( [ op for op in self.ops.keys() ] ) )

    def submit( self, request: Dict ) -> Task:
        filtered_request =  self.getFilteredRequest(request)
        self.logger.info( "Client {}: submit operations {}".format( self.client.name, str( filtered_request['operations'] ) ) )
        return self.client.request( filtered_request )

class Workflow:

    def __init__( self, **kwargs ):
        self.logger = StratusLogger.getLogger()
        self.opsets: Dict[str, OpSet] = {}
        self.graph = kwargs.get( "graph", nx.DiGraph( ) )
        for opset in kwargs.get( "opsets", [] ): self.add( opset )

    def addDependency(self, src: str, dest: str ):
        self.graph.add_edge( src, dest )
        edges = self.graph.edges
        return edges

    def __iter__(self) -> Iterator[OpSet]:
        return self.opsets.values().__iter__()

    def __hash__(self):
        return hash(repr(self))

    def ids(self) -> Set[str]:
        return set( self.opsets.keys() )

    def add(self, op: OpSet ):
        opsid = repr(op)
        self.opsets[ opsid ] = op
        self.graph.add_node( opsid )

    def __repr__(self):
        keys = list(self.opsets.keys())
        keys.sort()
        return "_".join(keys)

    def linkDependencies(self):
        for op in self.opsets.values():
            for vid in op.inputs:
                for op1 in self.opsets.values():
                    if op1.result ==  vid:
                        self.addDependency( op.id, op1.id )

    def remove(self, opsIds: List[str]):
        for opsid in opsIds:
            try:
                del self.opsets[ opsid ]
                self.graph.remove_node( opsid )
            except: pass

    def __eq__(self, other: "Workflow"):
        return repr( self ) == repr( other )


