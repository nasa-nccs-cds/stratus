import copy, os, time, traceback
from typing import List, Dict, Set, Iterator, Any, Optional
from stratus_endpoint.util.config import StratusLogger, UID
from stratus.app.client import StratusClient
from  stratus_endpoint.util.messaging import *
from concurrent.futures import wait, as_completed, Executor, Future
from stratus_endpoint.handler.base import TaskHandle, Endpoint, TaskResult, Status, FailedTask
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
        self._tid = kwargs.get( "tid", Endpoint.randomStr(6) )
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
    def tid(self) -> str:
        return self._tid

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
            self._taskHandle = self.client.request( filtered_request, self._tid, inputs  )
        return self._taskHandle

    @property
    def messages(self) -> RequestMetadata:
        return messageCenter.request( self._tid )

    def status(self) -> Status:
        if self._taskHandle is None: return Status.IDLE
        return self._taskHandle.status()

    def exception(self) -> Optional[ErrorRecord]:
        if self._taskHandle is None: return None
        return self.messages.error

class WorkflowTask(DGNode):

    def __init__( self, opset: ClientOpSet, **kwargs ):
        self._opset = opset
        inputs = [ conn.id for conn in opset.getInputs() ]
        outputs = [conn.id for conn in opset.getOutputs()]
        DGNode.__init__( self, inputs, outputs,  id=opset.tid, **kwargs )
        self.dependencies: List["WorkflowTask"] = None
        self._future: Future = None

    @property
    def messages(self) -> RequestMetadata:
        return messageCenter.request( self.tid )

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
    def tid(self) -> str:
        return self._opset.tid

    @property
    def handle(self) -> str:
        return self._opset.client.handle

    @property
    def type(self) -> str:
        return self._opset.type

    @property
    def taskHandle(self) -> TaskHandle:
        return self._opset.taskHandle

    def exception(self) -> Optional[ErrorRecord]:
        return self.messages.error

    def status(self) -> Status:
        return self._opset.status()

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
            self.messages.setException( err )
            self.logger.error( f"Error waiting on dependencies in task [{self.handle}:{self.rid}]: {repr(err)}")
            self.logger.error( traceback.format_exc() )
            return []

    def setDependencies( self, dependencies: List["WorkflowTask"] ):
        self.dependencies = dependencies

class Workflow(DependencyGraph):

    def __init__( self, rid: str, tasks: List[WorkflowTask], **kwargs ):
        DependencyGraph.__init__( self, tasks, **kwargs )
        self.result: TaskHandle = None
        self.completed_tasks = []
        self._rid = rid
        self.messages.setStatus( Status.IDLE )

    @property
    def messages(self) -> RequestMetadata:
        return messageCenter.request( self._rid )

    @DependencyGraph.add.register(WorkflowTask)
    def add( self, obj ):
        self._addDGNode( obj )

    def status(self) -> Status:
        return self.messages.status

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

    def getOutputTask( self ) -> WorkflowTask:
        outputTask: WorkflowTask = self.nodes.get( self.getOutputNode() )
        return outputTask

    def getResult(self) -> TaskHandle:
        return self.result

    def completed(self):
        return self.messages.status not in [Status.EXECUTING, Status.IDLE]

    @graphop
    def update( self ) -> bool:
        completed = True
        current_task = None
        try:
            wfStatus = self.status()
            if wfStatus in [Status.EXECUTING, Status.IDLE]:
                if wfStatus == Status.IDLE:
                    self.messages.setStatus(Status.EXECUTING)
                    self.logger.info( f"Initiating Execution of workflow {self._rid}")
                output_id = self.getOutputNode()
                for wtask in self.tasks:
                    current_task = wtask
                    if wtask.id not in self.completed_tasks:
                        tstat = wtask.status()
                        self.logger.info(f"Checking task[{wtask.taskHandle.__class__.__name__}:{wtask.id}] status: {tstat}")
                        if tstat == Status.ERROR:
                            errorRec = wtask.exception()
                            self.messages.setErrorRecord( errorRec )
                            raise Exception( "Workflow Errored out: " + errorRec.message  )
                        elif tstat == Status.CANCELED:
                            self.messages.setStatus( Status.CANCELED )
                            raise Exception("Workflow Canceled")
                        elif (tstat == Status.IDLE) and (wtask.dependentStatus() == Status.COMPLETED):
                            wtask.async_execute()
                            completed = False
                        elif ( tstat == Status.EXECUTING ):
                            completed = False
                        elif ( tstat == Status.COMPLETED ):
                            self.completed_tasks.append( wtask.id )
                            self.logger.info( f"COMPLETED TASK: taskID: {wtask.id}, outputID: {output_id}, nodes: {list(self.ids)}, exception: {wtask.taskHandle.exception()}, status: {wtask.taskHandle.status()}")
                            if wtask.id == output_id:
                                self.messages.setStatus(Status.COMPLETED)
                                self.result =  wtask.taskHandle
        except Exception as err:
            self.messages.setException( err )
            tid = current_task.tid if current_task else ""
            cid = current_task.cid if current_task else ""
            self.logger.error( getattr(err, 'message', repr(err)) )
            self.logger.error( "\n".join( traceback.format_tb( err.__traceback__ )))
            self.result = FailedTask( tid, self._rid, cid, err )
        return completed


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
