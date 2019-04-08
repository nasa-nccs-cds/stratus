import copy
from typing import List, Dict, Set, Iterator
from stratus.util.config import StratusLogger, UID
from app.client import StratusClient
from decorator import decorator
from stratus_endpoint.handler.base import Task
from stratus.app.graph import DGNode, DependencyGraph

class Op(DGNode):

    def __init__( self, **kwargs ):
        name_toks = kwargs.get("name").split(":")
        self.name: str = name_toks[-1]
        self.epas: List[str]  = name_toks[:-1]
        input_parm = kwargs.get("input")
        inputs:  List[str] = input_parm.split(",") if isinstance( input_parm, str ) else input_parm
        outputs: List[str] = [ kwargs.get( "result", UID.randomId( 6 ) ) ]
        DGNode. __init__( self, inputs, outputs, **kwargs )

class OpSet(DependencyGraph):

    def __init__( self, **kwargs ):
        DependencyGraph.__init__( **kwargs )

    def getFilteredRequest(self, request: Dict ) -> Dict:
        operations = []
        filtered_request = {}
        for node in self.nodes.values():
            op: Op = node
            operations.append( op.params )
            for epa in op.epas:
                for key,value in request.items():
                    parm_epas = key.split(":")[:-1]
                    if (len(parm_epas) == 0) or (epa in parm_epas):
                        filtered_request[key] = value
        filtered_request["operations"] = operations
        return filtered_request

    @DependencyGraph.add.register(Op)
    def add( self, obj ):
        self._addDGNode( obj )

class ClientOpSet(OpSet):

    def __init__( self, client: StratusClient, **kwargs ):
        OpSet.__init__( self,  **kwargs )
        self.client: StratusClient = client

    def connectedOpsets(self) -> List["ClientOpSet"]:
        subgraphs = self.connectedComponents()
        return [ self.filter( subgraph_iops ) for subgraph_iops in subgraphs ]

    def new(self) -> "ClientOpSet":
        return ClientOpSet(self.client)

    def copy(self) -> "ClientOpSet":
        return ClientOpSet( self.client, nodes=self.nodes.values(), graph = copy.deepcopy(self.graph) )

    @property
    def name(self) -> str:
        return self.client.name

    def __str__(self):
        return "C({}):[{}]".format( self.client, ",".join( [ op for op in self.nodes.keys() ] ) )

    def submit( self, request: Dict ) -> Task:
        filtered_request =  self.getFilteredRequest(request)
        self.logger.info( "Client {}: submit operations {}".format( self.client.name, str( filtered_request['operations'] ) ) )
        return self.client.request( filtered_request )

class WorkflowTask(DGNode):

    def __init__( self, opset: ClientOpSet, **kwargs ):
        self._opset = opset
        inputs = [ conn.id for conn in opset.getInputs() ]
        outputs = [conn.id for conn in opset.getOutputs()]
        DGNode. __init__( self, inputs, outputs, **kwargs )

class Workflow(DependencyGraph):

    def __init__( self, **kwargs ):
        DependencyGraph.__init__( **kwargs )

    @DependencyGraph.add.register(WorkflowTask)
    def add( self, obj ):
        self._addDGNode( obj )


