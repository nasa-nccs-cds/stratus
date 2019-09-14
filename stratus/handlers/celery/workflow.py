from stratus_endpoint.handler.base import  TaskHandle, Status, TaskResult, FailedTask
from stratus.app.operations import Workflow, WorkflowTask
from stratus.app.graph import DGNode, DependencyGraph, graphop, Connection
from stratus.app.client import StratusClient
from typing import Dict, List, Optional
import queue, datetime
from celery.utils.log import get_task_logger
from celery import Task
logger = get_task_logger(__name__)

class CeleryWorkflow( Workflow ):

    def __init__( self, **kwargs ):
        Workflow.__init__( self, **kwargs )
        self.celery_workflow = None


    def connect(self):
        Workflow.connect(self)
        for wtask in self.tasks:
            pass


    @graphop
    def update( self ) -> bool:
        completed = True
        try:
            if self._status in [Status.EXECUTING, Status.IDLE]:
                self._status = Status.EXECUTING
                output_id = self.getOutputNode()
                for wtask in self.tasks:
                    if wtask.id not in self.completed_tasks:
                        stat = wtask.status()
                        if stat == Status.ERROR:
                            self._status = Status.ERROR
                            exc = wtask.exception()
                            raise Exception( "Workflow Errored out: " + ( getattr(exc, 'message', repr(exc)) if exc is not None else "NULL" )  )
                        elif stat == Status.CANCELED:
                            self._status = Status.CANCELED
                            raise Exception("Workflow Canceled")
                        elif (stat == Status.IDLE) and (wtask.dependentStatus() == Status.COMPLETED):
                            wtask.async_execute()
                            completed = False
                        elif ( stat == Status.EXECUTING ):
                            completed = False
                        elif ( stat == Status.COMPLETED ):
                            self._status = Status.COMPLETED
                            self.completed_tasks.append( wtask.id )
                            self.logger.info( f"COMPLETED TASK: taskID: {wtask.id}, outputID: {output_id}, nodes: {list(self.ids)}, exception: {wtask.taskHandle.exception()}, status: {wtask.taskHandle.status()}")
                            if wtask.id == output_id:
                                self.result =  wtask.taskHandle
        except Exception as err:
            self._status = Status.ERROR
            self.result = FailedTask( err )
        return completed
