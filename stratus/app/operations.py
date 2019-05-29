import copy, os, time, traceback
from typing import List, Dict, Set, Iterator, Any, Optional
from stratus_endpoint.util.config import StratusLogger, UID
from stratus.app.client import StratusClient
from concurrent.futures import wait, as_completed, Executor, Future
from stratus_endpoint.handler.base import TaskHandle, TaskResult, TaskFuture, Status, FailedTask
from stratus.app.graph import DGNode, DependencyGraph, graphop, Connection

class Op(DGNode):

    def __init__( self, **kwargs ):
        inputs:  List[str] = self.parse( kwargs.get("input") )
        outputs: List[str] = [ kwargs.get( "result", UID.randomId( 6 ) ) ]
        DGNode. __init__( self, inputs, outputs, **kwargs )
        raw_name = self["name"]
        name_toks = raw_name.split(":") if ":" in raw_name else raw_name.split(".")
        self.name: str = name_toks[-1]
        self.epas: List[str]  = name_toks[:-1]
        self.logger.info( f"Creating OP Node, name: {self.name}, epas: {self.epas}")

    def parse(self, parm_value ) -> List[str]:
        if parm_value is None: return []
        elif isinstance(parm_value, str): return parm_value.split(",")
        elif isinstance(parm_value, (list, tuple)): return parm_value
        else: return [ str(parm_value) ]

class OpSet(DependencyGraph):

    def __init__( self, **kwargs ):
        DependencyGraph.__init__( self, **kwargs )

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
        self.logger.info( f"@@FR: operations = {operations}")
        return filtered_request

    @DependencyGraph.add.register(Op)
    def add( self, obj ):
        self._addDGNode( obj )

class ClientOpSet(OpSet):

    def __init__( self, request: Dict, client: StratusClient, **kwargs ):
        OpSet.__init__( self,  **kwargs )
        self.client: StratusClient = client
        self._request = request
        self._taskHandle: TaskHandle = None

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

    @property
    def taskHandle(self) -> TaskHandle:
        return self._taskHandle

    @property
    def source_cid(self) -> str:
        return self._request["cid"]

    @property
    def cid(self) -> str:
        return self.client.cid

    @property
    def rid(self) -> str:
        return self._request["rid"]

    @property
    def type(self) -> str:
        return self.client.type

    def __str__(self):
        return "C({}):[{}]".format( self.client, ",".join( [ op for op in self.nodes.keys() ] ) )

    def submit( self, inputs: List[TaskResult] ) -> TaskHandle:
        self.logger.info(f"ClientOpSet Submit TASK {self.client.handle}" )
        if self._taskHandle is None:
            filtered_request =  self.getFilteredRequest( self._request )
            self.logger.info( f"Client {self.client.handle}: submit operations {filtered_request['operations']}" )
            self._taskHandle = self.client.request(filtered_request, inputs)
        return self._taskHandle

    def status(self) -> Status:
        if self._taskHandle is None: return Status.IDLE
        return self._taskHandle.status()

    def exception(self) -> Exception:
        if self._taskHandle is None: return None
        return self._taskHandle.exception()

class WorkflowTask(DGNode):

    def __init__( self, opset: ClientOpSet, **kwargs ):
        self._opset = opset
        inputs = [ conn.id for conn in opset.getInputs() ]
        outputs = [conn.id for conn in opset.getOutputs()]
        DGNode.__init__( self, inputs, outputs, **kwargs )
        self.dependencies: List["WorkflowTask"] = None
        self._future: Future = None

    @property
    def name(self) -> str:
        return self._opset.name

    @property
    def cid(self) -> str:
        return self._opset.cid

    @property
    def source_cid(self) -> str:
        return self._opset.source_cid

    @property
    def rid(self) -> str:
        return self._opset.rid

    @property
    def handle(self) -> str:
        return self._opset.client.handle

    @property
    def type(self) -> str:
        return self._opset.type

    @property
    def taskHandle(self) -> TaskHandle:
        return self._opset.taskHandle

    def exception(self) -> Exception:
        return self._opset.exception()

    def status(self) -> Status:
        return self._opset.status()

    def submit( self, executor: Executor, **kwargs ) -> TaskFuture:
        self.logger.info( f"Submitting Task[{self.handle}:{self.rid}]")
        self._future = executor.submit( self.execute, **kwargs )
        tparms = { "rid":self.rid, "cid":self.cid, **kwargs }
        return TaskFuture( self._future, **tparms )

    def execute( self, **kwargs ) -> TaskResult:
        results: List[TaskResult] = self.waitOnTasks()
        handle = self._opset.submit( results )
        return handle.blockForResult( **kwargs )

    def getDependentInputs(self) -> List[TaskResult]:
        results = []
        for dep in self.dependencies:
            taskHandle = dep.taskHandle
            assert taskHandle is not None, f"[{self.handle}] Workflow execution error: dependency not executed: {dep.name}"
            result = taskHandle.getResult()
            assert result is not None, f"[{self.handle}] Workflow execution error: dependency not completed: {dep.name}"
            results.append( result )
        return results

    def async_execute( self, **kwargs ) -> TaskHandle:
        if self.taskHandle is None:
            results = self.getDependentInputs()
            self._opset.submit( results )
        return self.taskHandle

    def getFuture(self) -> Future:
        while True:
            if self._future is not None: return self._future
            time.sleep( 0.05 )

    def dependentStatus(self) -> Status:
        dstat_list = [ dep.status() for dep in self.dependencies ]
        for dstat in dstat_list:
            if dstat in [ Status.ERROR, Status.CANCELED ]: return dstat
        for dstat in dstat_list:
            if dstat in [ Status.EXECUTING, Status.IDLE ]: return dstat
        return Status.COMPLETED

    def waitOnTasks( self ) -> List[TaskResult]:
        try:
            assert self.dependencies is not None, "Must call setDependencies before waitOnTasks"
            self.logger.info(f"START WaitOnTasks[{self.handle}], dep = {[ dep.handle for dep in self.dependencies]}")
            futures: List[Future] = [ dep.getFuture() for dep in self.dependencies ]
            wait(futures)
            taskResults: List[TaskResult] = [ future.result() for future in futures ]
            for taskResult in taskResults:
                self.logger.info(f"TASK[{self.handle}]: Got Dependency RESULT-> empty: {taskResult.empty()}, header = {taskResult.header}")
            return taskResults
        except Exception as err:
            self.logger.error( f"Error waiting on dependencies in task [{self.handle}:{self.rid}]: {repr(err)}")
            self.logger.error( traceback.format_exc() )
            return []

    def setDependencies( self, dependencies: List["WorkflowTask"] ):
        self.dependencies = dependencies

class WorkflowExeFuture:

    def __init__( self, request: Dict, task_futures: Dict[str,TaskFuture], **kwargs ):
        self.rid = request['rid']
        self.futures: Dict[str,TaskFuture] = task_futures
        self.request: Dict = request
        self._exception: Exception = None

    def cancel(self):
        for tfuture in self.futures.values(): tfuture.cancel()

    def exception(self) -> Exception:
        return self._exception

    def getResult( self, **kwargs ) -> TaskResult:
        results = []
        for tid,tfuture in self.futures.items():
            result = tfuture.getResult( **kwargs )
            if results is not None: results.append(result)
        return TaskResult.merge(results)

    def status(self) -> Status:
        completed = True
        for tfuture in self.futures.values():
            status = tfuture.status()
            if status == Status.ERROR:
                self.cancel()
                self._exception = tfuture.exception()
                return Status.ERROR
            elif status == Status.CANCELED:
                self.cancel()
                return Status.CANCELED
            elif status == Status.EXECUTING:
                completed = False
        return Status.COMPLETED if completed else Status.EXECUTING

    def cid(self):
        return self.request["cid"]

    def get(self, name: str, default = None ) -> Any:
        return self.request.get( name, default )

    def __getitem__( self, key: str ) -> Any:
        return  self.request.get( key, None )

class Workflow(DependencyGraph):

    def __init__( self, **kwargs ):
        DependencyGraph.__init__( self, **kwargs )

    @DependencyGraph.add.register(WorkflowTask)
    def add( self, obj ):
        self._addDGNode( obj )

    def connect(self):
        DependencyGraph.connect(self)
        for wtask in self.tasks:
            in_edges = self.graph.in_edges(wtask.id)
            connections = [ Connection(self.graph.get_edge_data(*edge_tup)["id"], edge_tup[0], edge_tup[1]) for edge_tup in in_edges ]
            nids = [conn.nid(Connection.INCOMING) for conn in connections ]
            dep_tasks: List[WorkflowTask] =  [self.nodes.get(nid) for nid in nids if nid is not None]
            wtask.setDependencies( dep_tasks )

    @property
    def tasks(self) -> List[WorkflowTask]:
        wtasks: List[WorkflowTask] = list(self.nodes.values())
        return wtasks

    @graphop
    def submitExec( self, executor: Executor ) -> Dict[str,TaskFuture]:
        results: Dict[str,TaskFuture] = { wtask.id: wtask.submit(executor) for wtask in self.tasks }
        return { tid:fut for tid,fut in results.items() if tid in self.getOutputNodes() }

    @graphop
    def submit( self ) -> Dict[str,TaskHandle]:
        results = {}
        output_ids = self.getOutputNodes()
        completed_tasks = []
        while True:
            completed = True
            for wtask in self.tasks:
                if wtask.id not in completed_tasks:
                    stat = wtask.status()
                    if stat == Status.ERROR:
                        results[wtask.id] = FailedTask( wtask.exception() )
                        self.logger.info(f"Failed TASK: taskID: {wtask.id}, outputIDs: {output_ids}, nodes: {list(self.ids)}")
                        completed_tasks.append(wtask.id)
                    elif stat == Status.CANCELED:
                        results[wtask.id] = FailedTask( Exception( "Task canceled") )
                        self.logger.info(f"Cancelled TASK: taskID: {wtask.id}, outputIDs: {output_ids}, nodes: {list(self.ids)}")
                        completed_tasks.append(wtask.id)
                    elif (stat == Status.IDLE) and (wtask.dependentStatus() == Status.COMPLETED):
                        taskHandle = wtask.async_execute()
                        self.logger.info( f"COMPLETED TASK: taskID: {wtask.id}, outputIDs: {output_ids}, nodes: {list(self.ids)}")
                        if wtask.id in output_ids:
                            results[ wtask.id ] =  taskHandle
                        completed = False
                    elif ( stat == Status.EXECUTING ):
                        completed = False
                    elif ( stat == Status.COMPLETED ):
                        completed_tasks.append( wtask.id )
            if completed: break
            time.sleep(0.02)
        return results

if __name__ == "__main__":
    from stratus.app.core import StratusCore
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
    task: TaskHandle = client.request( request )
    time.sleep(1.5)
    status = task.status()
    print ( "status response = " + str(status))
