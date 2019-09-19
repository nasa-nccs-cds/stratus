from stratus_endpoint.handler.base import  TaskHandle, Status, TaskResult, FailedTask
from stratus.app.operations import WorkflowBase, WorkflowTask
from stratus.app.graph import DGNode, DependencyGraph, graphop, Connection
from celery.result import AsyncResult
from stratus_endpoint.util.config import StratusLogger
from celery import group, states
from stratus.app.client import StratusClient
from typing import Dict, List, Optional
import queue, datetime
from celery.utils.log import get_task_logger
from celery import Task
logger = get_task_logger(__name__)

class CeleryTaskHandle(TaskHandle):

    def __init__(self, rid: str, cid: str, manager: AsyncResult, **kwargs):
        TaskHandle.__init__( self, rid=rid, cid=cid, **kwargs )   # **{ "rid":rid, "cid":cid, **kwargs }
        self.logger = StratusLogger.getLogger()
        self.manager: AsyncResult = manager
        self._exception = None

    def getResult( self, **kwargs ) ->  Optional[TaskResult]:
        timeout = kwargs.get("timeout",None)
        block = kwargs.get("block",False)
        try:
            if block:
                return self.manager.get( timeout )
            if self.manager.ready():
                if self.manager.successful():
                    return self.manager.result
                elif self.manager.failed():
                    self._exception = self.manager.result
                    return None
            else: return None
        except Exception as err:
            self._exception = err
            raise err

    def status(self) ->  Status:
        if self.manager.failed(): return Status.ERROR
        elif self.manager.successful(): return Status.COMPLETED
        elif self.manager.state in [ states.STARTED, states.RETRY]: return Status.EXECUTING
        elif self.manager.state in [states.PENDING, states.RECEIVED]: return Status.IDLE
        elif self.manager.state in [states.REJECTED, states.REVOKED]: return Status.CANCELED
        else: return Status.UNKNOWN

    def exception(self) -> Optional[Exception]:
        if self._exception is None and self.manager.failed():
            self._exception = self.manager.result
        return self._exception


class CeleryWorkflow(WorkflowBase):

    def __init__( self, **kwargs ):
        WorkflowBase.__init__(self, **kwargs)
        self.taskSigs: Dict = {}
        self.celery_workflow_sig = None
        self.celery_result: AsyncResult = None
        self.cid = None
        self.rid = None

    def getConnectedTaskSig( self, wtask: WorkflowTask ):
        from .app import celery_execute
        if wtask.id not in self.taskSigs:
            core_task_sig = celery_execute.s(wtask.clientSpec, wtask.requestSpec)
            dep_sigs = [ self.getConnectedTaskSig(deptask) for deptask in wtask.dependencies ]
            if len(dep_sigs) == 0:      self.taskSigs[wtask.id] =                       core_task_sig
            elif len( dep_sigs ) == 1:  self.taskSigs[wtask.id] = (    dep_sigs[0]    | core_task_sig )
            else:                       self.taskSigs[wtask.id] = ( group( dep_sigs ) | core_task_sig )
        return self.taskSigs[wtask.id]

    def connect(self):
        wtask: WorkflowTask
        WorkflowBase.connect(self)
        for wtask in self.tasks:
            if self.cid == None:
                self.cid, self.rid = wtask.cid, wtask.rid
            out_edges = self.graph.out_edges(wtask.id)
            connections = [Connection(self.graph.get_edge_data(*edge_tup)["id"], edge_tup[0], edge_tup[1]) for edge_tup in out_edges]
            nids = [conn.nid(Connection.OUTGOING) for conn in connections]
            consumer_tasks: List[WorkflowTask] = [self.nodes.get(nid) for nid in nids if nid is not None]
            wtask.setConsumers(consumer_tasks)
        for wtask in self.tasks:
            self.celery_workflow_sig = self.getConnectedTaskSig( wtask )

    @graphop
    def update( self ) -> bool:

        if self.celery_result == None:
            task_inputs = []
            self.celery_result = self.celery_workflow_sig.apply_async( args=[ task_inputs ] )
            self.result = CeleryTaskHandle( self.rid, self.cid, self.celery_result )
            self._status = Status.EXECUTING
        else:
            if self.celery_result.successful():
                self._status = Status.COMPLETED
                return True
            elif self.celery_result.failed():
                self._status = Status.ERROR
                exc = self.celery_result.result
                raise Exception("Workflow Errored out: " + (getattr(exc, 'message', repr(exc)) if exc is not None else "NULL"))
        return False


        # completed = True
        # try:
        #     if self._status in [Status.EXECUTING, Status.IDLE]:
        #         self._status = Status.EXECUTING
        #         output_id = self.getOutputNode()
        #         for wtask in self.tasks:
        #             if wtask.id not in self.completed_tasks:
        #                 stat = wtask.status()
        #                 if stat == Status.ERROR:
        #                     self._status = Status.ERROR
        #                     exc = wtask.exception()
        #                     raise Exception( "Workflow Errored out: " + ( getattr(exc, 'message', repr(exc)) if exc is not None else "NULL" )  )
        #                 elif stat == Status.CANCELED:
        #                     self._status = Status.CANCELED
        #                     raise Exception("Workflow Canceled")
        #                 elif (stat == Status.IDLE) and (wtask.dependentStatus() == Status.COMPLETED):
        #                     wtask.async_execute()
        #                     completed = False
        #                 elif ( stat == Status.EXECUTING ):
        #                     completed = False
        #                 elif ( stat == Status.COMPLETED ):
        #                     self._status = Status.COMPLETED
        #                     self.completed_tasks.append( wtask.id )
        #                     self.logger.info( f"COMPLETED TASK: taskID: {wtask.id}, outputID: {output_id}, nodes: {list(self.ids)}, exception: {wtask.taskHandle.exception()}, status: {wtask.taskHandle.status()}")
        #                     if wtask.id == output_id:
        #                         self.result =  wtask.taskHandle
        # except Exception as err:
        #     self._status = Status.ERROR
        #     self.result = FailedTask( err )
        # return completed
