from celery.backends.database.models import Task as CeleryTask
import networkx as nx
from sqlalchemy import Column, Integer, String
from networkx.algorithms.dag import is_directed_acyclic_graph
from sqlalchemy_json import MutableJson

class Workflow:
    __tablename__ = 'workflow'
    id = Column(Integer, primary_key=True)
    dag_adjacency_list = Column(MutableJson)

    @property
    def execution_graph(self):
        d = self.dag_adjacency_list
        G = nx.DiGraph()

        for node in d.keys():
            nodes = d[node]
            if len(nodes) == 0:
                G.add_node(int(node))
                continue
            G.add_edges_from([(int(node), n) for n in nodes])
        return G

    def find_entry_point(G):
        result = []
        for node in G.nodes:
            if len(list(G.predecessors(node))) == 0:
                result.append(node)
        return result

    def _is_node_rdy(task, graph):
        tasks = session.query(Task).filter(Task.id.in_(list(graph.predecessors(task.id)))).all()
        for dep_task in tasks:
            if not dep_task.celery_task_uid or \
               not AsyncResult(dep_task.celery_task_uid).state == SUCCESS:
                return False
        return True

    def _process_task_node(task, uid):
        task.celery_task_uid = uid
        session.add(task)
        session.commit()

        # simulate that task runs
        for i in range(task.sleep):
            print('{}: Sleep, sec: {}'.format(uid, i))
            time.sleep(1)

    def run(self, workflow_id, cur_task_id=None):
        print('Runnning Workflow {} and Task {}'.format(workflow_id, cur_task_id))
        workflow = session.query(Workflow).filter_by(id=workflow_id).one()
        graph = workflow.execution_graph

        next_task_ids = []
        if cur_task_id:
            task = session.query(Task).get(cur_task_id)
            if not _is_node_rdy(task, graph):
                return

            _process_task_node(task, self.request.id)

            next_task_ids = list(graph.successors(cur_task_id))
        else:
            next_task_ids = find_entry_point(graph)

        self.update_state(state=SUCCESS)

        for task_id in next_task_ids:
            run.apply_async( args=(workflow_id, task_id,), queue=QUEUE_NAME )

class Task(Base):
    __tablename__ = 'task'
    id = Column(Integer, primary_key=True)
    celery_task_uid = Column(String(100))
    sleep = Column(Integer)