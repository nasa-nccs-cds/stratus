import copy, abc
from typing import List, Dict, Set, Iterator, Any
from stratus_endpoint.util.config import StratusLogger, UID
from stratus.app.client import StratusClient
from decorator import decorator, dispatch_on
from stratus_endpoint.handler.base import TaskHandle
import networkx as nx

@decorator
def graphop( func, *args, **kwargs ):
    args[0].connect()
    return func( *args, **kwargs)

class Connection:
    INCOMING = 0
    OUTGOING = 1
    ALL = 2

    def __init__(self, id, src_nid, dest_nid ):
        self._id = id
        self._src_nid = src_nid
        self._dest_nid = dest_nid

    @property
    def id(self): return self._id

    @property
    def src_nid(self): return self._src_nid

    def nid( self, type: int ):
        if type == self.INCOMING: return self._src_nid
        elif type == self.OUTGOING: return self._dest_nid
        else: raise Exception( f"Illegal connection type: {type}")

    @property
    def dest_nid(self): return self._dest_nid

    def __repr__(self):
        return f"C[{self._id}:{self._src_nid}->{self._dest_nid}]"

class DGNode:

    def __init__( self, inputs: List[str], outputs: List[str] = None, **kwargs ):
        self.logger =  StratusLogger.getLogger()
        self.params: Dict[str,Any] = kwargs
        self.id = self.get( "id", UID.randomId( 6 ) )
        self._inputs = inputs
        self._outputs = outputs if outputs is not None else [ UID.randomId(6) ]

    @abc.abstractmethod
    def getInputs(self)-> List[str]:
        return self._inputs

    @abc.abstractmethod
    def getOutputs(self)-> List[str]:
        return self._outputs

    def get(self, name: str, default ) -> Any:
        return self.params.get( name, default )

    def __getitem__( self, key: str ) -> Any:
        result =  self.params.get( key, None )
        assert result is not None,f"Missing required parameter in DGNode {self.id}: {key}, parms = {self.params}"
        return result

class DependencyGraph():

    def __init__( self, **kwargs ):
        self.logger = StratusLogger.getLogger()
        self.nodes: Dict[str, DGNode] = {}
        self.graph = kwargs.get( "graph", nx.DiGraph( ) )
        for node in kwargs.get( "nodes", [] ): self.add( node )
        self._allow_multiple_outputs = kwargs.get( "allow_multiple_outputs", False )
        self._connected = False

    def _addDependency(self, depId, srcId: str, destId: str ):
        if not self.graph.has_edge( srcId, destId ):
            self.logger.debug( f"Add Connection[{depId}]: {srcId} -> {destId}")
            self.graph.add_edge( srcId, destId, id=depId )

    def __iter__(self) -> Iterator[DGNode]:
        return self.nodes.values().__iter__()

    def __hash__(self):
        return hash(repr(self))

    @property
    def ids(self) -> Set[str]:
        return set(self.nodes.keys())

    @dispatch_on('obj')
    def add( self, obj ):
        raise NotImplementedError(type(obj))

    def _addDGNode(self, node: DGNode):
        if node.id not in self.nodes.keys():
            self._connected = False
            self.nodes[node.id] = node

    def __repr__(self):
        keys = list(self.nodes.keys())
        keys.sort()
        return "-".join(keys)

    def connect(self):
        if not self._connected:
            self.graph.clear()
            for nid in self.nodes.keys(): self.graph.add_node(nid)
            for dnode in self.nodes.values():
                inputs = dnode.getInputs()
                self.logger.debug(f"Checking dest node[{dnode.id}] inputs: {inputs}")
                for iid in inputs:
                    for snode in self.nodes.values():
                        outputs = snode.getOutputs()
                        self.logger.debug(f"Checking src node[{snode.id}] outputs: {outputs}")
                        if iid in outputs:
                            self._addDependency( iid, snode.id, dnode.id  )
            self._connected = True

    def remove(self, nids: List[str]):
        for nid in nids:
            try:
                del self.nodes[ nid]
                self.graph.remove_node( nid )
            except: pass

    def copy(self):
        return DependencyGraph( nodes=self.nodes.values(), graph = copy.deepcopy(self.graph) )

    def filter(self, iops: Set[str] ) -> "DependencyGraph":
        newDepGraph = self.copy()
        newDepGraph.remove( list( self.ids ^ iops ) )
        return newDepGraph

    def __len__(self):
        return self.nodes.__len__()

    def __eq__(self, other: "DependencyGraph"):
        return hash(self) == hash(other)

    def __lt__(self, other: "DependencyGraph"):
        return len(self.nodes) < len(other)

    @graphop
    def connectedComponents(self):
        components = nx.weakly_connected_components(self.graph)
        return [subgraph_iops for subgraph_iops in components]

    @graphop
    def predecessors( self, nid: str ):
        return self.graph.predecessors( nid )

    @graphop
    def has_predecessor( self, nid0: str,  nid1: str ):
        return self.graph.has_predecessor( nid0, nid1 )

    @graphop
    def successors( self, nid: str ):
        return self.graph.successors( nid )

    @graphop
    def has_successor( self, nid0: str,  nid1: str ):
        return self.graph.has_successor( nid0, nid1 )

    @graphop
    def getConnections( self, nid: str, ctype: int ) -> List[Connection]:
        connections = []
        if ctype == Connection.INCOMING: graph_edges = self.graph.in_edges(nid)
        elif ctype == Connection.OUTGOING: graph_edges = self.graph.out_edges(nid)
        elif ctype == Connection.ALL: graph_edges = self.graph.edges(nid)
        else: raise Exception( "Unknown Connections type: {ctype}")
        for edge_tup in graph_edges:
            edata = self.graph.get_edge_data(*edge_tup)
            connections.append( Connection( edata["id"], edge_tup[0], edge_tup[1] ) )
        return connections

    @graphop
    def getConnectedNodes(self, nid: str, ctype: int) -> List[DGNode]:
        nids = [ conn.nid(ctype) for conn in  self.getConnections( nid, ctype ) ]
        return [self.nodes.get( nid ) for nid in nids if nid is not None ]

    @graphop
    def getInputs(self) -> List[Connection]:
        ilist = []
        for nid,dnode  in self.nodes.items():
            incoming_connection_ids = [ conn.id for conn in self.getConnections( nid, Connection.INCOMING ) ]
            for iid in dnode.getInputs():
                if iid not in incoming_connection_ids: ilist.append( Connection(iid,None,nid) )
        return ilist

    @graphop
    def getOutputs(self) -> List[Connection]:
        olist = []
        for nid,dnode  in self.nodes.items():
            outgoing_connection_ids = [ conn.id for conn in self.getConnections( nid, Connection.OUTGOING ) ]
            for oid in dnode.getOutputs():
                if oid not in outgoing_connection_ids: olist.append( Connection(oid,nid,None) )
        if not self._allow_multiple_outputs:
            if len( olist ) > 1:
                raise Exception( f"Multiple output nodes in workflow (not allowed), olist = {[c.__repr__() for c in olist]}, outgoing_connection_ids={outgoing_connection_ids}")
        if len(olist) == 0: raise Exception("Missing output node in workflow")
        return olist

    @graphop
    def getOutputNodes(self) -> List[str]:
        return [ conn.src_nid for conn in self.getOutputs() ]

    @graphop
    def getOutputNode(self) -> str:
        outputs = self.getOutputs()
        return outputs[0].src_nid

if __name__ == "__main__":
    dgraph = DependencyGraph()
    dgraph._addDGNode( DGNode( inputs=[ "s0", "s1" ], outputs=[  "r1", "r2" ], id="n0" ) )
    dgraph._addDGNode( DGNode( inputs=[ "r1" ], outputs=["r11", "r12"], id="n1"))
    dgraph._addDGNode( DGNode( inputs=[ "r2" ], outputs=["r21"], id="n2"))
    dgraph._addDGNode( DGNode( inputs=["s3"], outputs=["r5"], id="n3"))

    print(dgraph.getInputs())
    print(dgraph.getOutputs())
    print(dgraph.connectedComponents())
