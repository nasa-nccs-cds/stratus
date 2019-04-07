import copy, abc
from typing import List, Dict, Set, Iterator, Any
from stratus.util.config import StratusLogger, UID
from app.client import StratusClient
from decorator import decorator
from stratus_endpoint.handler.base import Task
import networkx as nx

@decorator
def graphop( func, *args, **kwargs ):
    args[0].connect()
    return func( *args, **kwargs)

class DGNode:
    __metaclass__ = abc.ABCMeta

    def __init__( self, **kwargs ):
        self.params: Dict[str,Any] = kwargs
        self.id = self.get( "id", UID.randomId( 6 ) )

    @abc.abstractmethod
    def inputs(self)-> List[str]: pass

    @abc.abstractmethod
    def outputs(self)-> List[str]: pass

    def get(self, name: str, default = None ) -> Any:
        parm = self.params.get( name, default )
        if parm is None: raise Exception( f"Missing required parameter in DGNode {self.id}: {name}" )
        return parm

    def __getitem__( self, key: str ) -> Any:
        result =  self.params.get( key, None )
        assert result is not None,f"Missing required parameter in DGNode {self.id}: {key}"
        return result

class TestDGNode(DGNode):

    def __init__( self, **kwargs ):
        DGNode.__init__( self, **kwargs )

    def inputs(self)-> List[str]:
        return self.get( "inputs ", [] )

    def outputs(self)-> List[str]:
        return self.get( "outputs ", [] )

class DependencyGraph():

    def __init__( self, **kwargs ):
        self.logger = StratusLogger.getLogger()
        self.nodes: Dict[str, DGNode] = {}
        self.graph = kwargs.get( "graph", nx.DiGraph( ) )
        for node in kwargs.get( "nodes", [] ): self.add( node )
        self._connected = False

    def addDependency(self, srcId: str, destId: str ):
        if not self.graph.has_edge( srcId, destId ):
            self.graph.add_edge( srcId, destId )
            self._connected = False

    def __iter__(self) -> Iterator[DGNode]:
        return self.nodes.values().__iter__()

    def __hash__(self):
        return hash(repr(self))

    def ids(self) -> Set[str]:
        return set(self.nodes.keys())

    def add(self, node: DGNode):
        if node.id not in self.nodes.keys():
            self._connected = False
            self.nodes[node.id] = node
            self.graph.add_node(node.id)

    def __repr__(self):
        keys = list(self.nodes.keys())
        keys.sort()
        return "-".join(keys)

    def connect(self):
        if not self._connected:
            for dnode in self.nodes.values():
                for iid in dnode.inputs():
                    for snode in self.nodes.values():
                        if iid in snode.outputs:
                            self.addDependency( snode.id, dnode.id )
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
        newDepGraph.remove( list( self.ids() ^  iops ) )
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
    def inputs(self) -> List[str]:
        ilist = []
        for dnode in self.nodes.values():
            for iid in dnode.inputs():
                inlinks = self.predecessors( iid )
                if len( inlinks ) == 0: ilist.append( iid )
        return ilist

    @graphop
    def outputs(self) -> List[str]:
        olist = []
        for dnode in self.nodes.values():
            for oid in dnode.outputs():
                outlinks = self.successors( oid )
                if len( outlinks ) == 0: olist.append( oid )
        return olist

if __name__ == "__main__":
    dgraph = DependencyGraph()
    dgraph.add( TestDGNode( id="n0", inputs="s0,s1", outputs=[  "r1", "r2" ]) )
    dgraph.add( TestDGNode( id="n1", inputs=[ "r1" ], outputs=["r11", "r12"]))
    dgraph.add( TestDGNode( id="n2", inputs=[ "r2" ], outputs=["r21"]))

    print( dgraph.inputs() )
    print( dgraph.outputs() )