import os, json, yaml, abc, copy, sys
from typing import List, Union, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional, Set, Tuple, Iterator, Iterable
from stratus.util.config import Config, StratusLogger, UID
from stratus.handlers.client import StratusClient
from stratus_endpoint.handler.base import Task, Status
import networkx as nx

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

    def get(self, name: str, default = None ) -> str:
        parm = self.params.get( name, default )
        if parm is None: raise Exception( "Missing required parameter in operation: " + name )
        return parm

    def __getitem__( self, key: str ) -> str:
        result =  self.params.get( key, None )
        assert result is not None, f"Missing required parameter in operation: {key} "
        return result

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

    def linkDependencies(self):
        for op in self.ops.values():
            for vid in op.inputs:
                for op1 in self.ops.values():
                    if op1.result ==  vid:
                        self.addDependency( op.id, op1.id )

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

    def connectedComponents(self) -> List["ClientOpSet"]:
        self.linkDependencies()
        components = nx.weakly_connected_components(self.graph)
        subgraphs = [ subgraph_iops for subgraph_iops in components ]
        return [ self.filter( subgraph_iops ) for subgraph_iops in subgraphs ]

    def copy(self) -> "ClientOpSet":
        return ClientOpSet( self.client, ops=self.ops.values(), graph = self.graph )

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
