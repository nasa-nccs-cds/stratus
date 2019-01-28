from celery.backends.database.models import Task as CeleryTask
import networkx as nx
from sqlalchemy import Column, Integer, String
from networkx.algorithms.dag import is_directed_acyclic_graph
from sqlalchemy_json import MutableJson
from stratus.dbase.models import Workflow, Task
from stratus.app import app
from celery.states import SUCCESS
from celery.result import AsyncResult
import time

@app.app.task(bind=True)
def run( self, workflow_id, cur_task_id=None ):
    print('Runnning Workflow {} and Task {}'.format(workflow_id, cur_task_id))
    workflow = Workflow.query.filter_by(id=workflow_id).one()
    graph = workflow.execution_graph

    next_task_ids = []
    if cur_task_id:
        task = Task.query.get(cur_task_id)
        if not Task._is_node_rdy(task, graph):
            return

        Task._process_task_node(task, self.request.id )

        next_task_ids = list(graph.successors(cur_task_id))
    else:
        next_task_ids = Workflow.find_entry_point(graph)

    self.update_state(state=SUCCESS)

    for task_id in next_task_ids:
        run.apply_async( args=(workflow_id, task_id,), queue=self.QUEUE_NAME )

