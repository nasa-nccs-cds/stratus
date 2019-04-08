import copy, os, time
from typing import List, Dict, Set, Iterator
from stratus.util.config import StratusLogger, UID
from app.client import StratusClient
from stratus_endpoint.handler.base import TaskFuture, TaskResult
from stratus.app.graph import DGNode, DependencyGraph, graphop, Connection

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

    def __init__( self, request: Dict, client: StratusClient, **kwargs ):
        OpSet.__init__( self,  **kwargs )
        self.client: StratusClient = client
        self._request = request
        self._future = None

    def connectedOpsets(self) -> List["ClientOpSet"]:
        subgraphs = self.connectedComponents()
        return [ self.filter( subgraph_iops ) for subgraph_iops in subgraphs ]

    def new(self) -> "ClientOpSet":
        return ClientOpSet( self._request, self.client )

    def copy(self) -> "ClientOpSet":
        return ClientOpSet( self._request, self.client, nodes=self.nodes.values(), graph = copy.deepcopy(self.graph) )

    @property
    def name(self) -> str:
        return self.client.name

    def __str__(self):
        return "C({}):[{}]".format( self.client, ",".join( [ op for op in self.nodes.keys() ] ) )

    def submit( self, inputs: List[TaskResult] ) -> TaskFuture:
        if self._future is None:
            filtered_request =  self.getFilteredRequest( self._request )
            self.logger.info( "Client {}: submit operations {}".format( self.client.name, str( filtered_request['operations'] ) ) )
            self._future = self.client.request( filtered_request, inputs )
        return self._future

class WorkflowTask(DGNode):

    def __init__( self, opset: ClientOpSet, **kwargs ):
        self._opset = opset
        inputs = [ conn.id for conn in opset.getInputs() ]
        outputs = [conn.id for conn in opset.getOutputs()]
        DGNode.__init__( self, inputs, outputs, **kwargs )
        self.dependencies: List["WorkflowTask"] = None

    async def submit(self ) -> TaskFuture:
        results: List[TaskResult] = await self.waitOnTasks()
        return self._opset.submit( results )

    async def waitOnTasks( self ) -> List[TaskResult]:
        assert self.dependencies is not None, "Must call setDependencies before waitOnTasks"
        results: List[TaskResult] = []
        for wTask in self.dependencies:
            tFuture: TaskFuture = await wTask.submit()
            result: TaskResult = await tFuture.blockForResult()
            results.append( result )
        return results

    def setDependencies( self, dependencies: List["WorkflowTask"] ):
        self.dependencies = dependencies

class Workflow(DependencyGraph):

    def __init__( self, **kwargs ):
        DependencyGraph.__init__( **kwargs )

    @DependencyGraph.add.register(WorkflowTask)
    def add( self, obj ):
        self._addDGNode( obj )

    @property
    def tasks(self) -> List[WorkflowTask]:
        wtasks: List[WorkflowTask] = list(self.nodes.values())
        return wtasks

    @graphop
    async def submit(self) -> Dict[str,TaskFuture]:
        for wtask in self.tasks:
            dep_tasks: List[WorkflowTask] = self.getConnectedNodes( wtask.id, Connection.INCOMING )
            wtask.setDependencies( dep_tasks )
        results = { wtask.id: await wtask.submit() for wtask in self.tasks }
        return results


if __name__ == "__main__":
    from app.core import StratusCore
    from stratus.util.test import TestDataManager as mgr
    settings = dict( stratus=dict( type="endpoint" ) )

    core = StratusCore( settings )
    client = core.getClient()
    client.init()
    request = dict(
        domain=[{"name": "d0", "lat": {"start": 50, "end": 55, "system": "values"},
                 "lon": {"start": 40, "end": 42, "system": "values"},
                 "time": {"start": "1980-01-01", "end": "1981-12-31", "crs": "timestamps"}}],
        input=[{"uri": mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}],
        operation=[ { "epa": "test.subset", "input": "v0"} ]
    )
    task: TaskFuture = client.request( request )
    time.sleep(1.5)
    status = task.status()
    print ( "status response = " + str(status))
