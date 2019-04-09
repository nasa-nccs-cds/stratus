# import networkx as nx
# from sqlalchemy import Column, Integer, String
# from sqlalchemy_json import MutableJson
# from celery.states import SUCCESS
# from celery.result import AsyncResult
# import time
#
# class Workflow(app.db.Model):
#     __tablename__ = 'workflow'
#     id = Column(Integer, primary_key=True)
#     dag_adjacency_list = Column(MutableJson)
#     QUEUE_NAME = app.config['QUEUE_NAME']
#
#     @property
#     def execution_graph(self) -> nx.DiGraph:
#         d = self.dag_adjacency_list
#         G = nx.DiGraph()
#
#         for node in d.keys():
#             nodes = d[node]
#             if len(nodes) == 0:
#                 G.add_node(int(node))
#                 continue
#             G.add_edges_from([(int(node), n) for n in nodes])
#         return G
#
#     @classmethod
#     def find_entry_point( cls, G: nx.DiGraph ):
#         result = []
#         for node in G.nodes:
#             if len(list(G.predecessors(node))) == 0:
#                 result.append(node)
#         return result
#
#
# class TaskHandle(app.db.Model):
#     __tablename__ = 'task'
#     id = Column(Integer, primary_key=True)
#     celery_task_uid = Column(String(100))
#     sleep = Column(Integer)
#
#     @classmethod
#     def _is_node_rdy(cls, task, graph):
#         tasks = TaskHandle.query.filter(TaskHandle.id.in_(list(graph.predecessors(task.id)))).all()
#         for dep_task in tasks:
#             if not dep_task.celery_task_uid or \
#                not AsyncResult(dep_task.celery_task_uid).state == SUCCESS:
#                 return False
#         return True
#
#     @classmethod
#     def _process_task_node(cls, task, uid):
#         task.celery_task_uid = uid
#         app.db.session.add(task)
#         app.db.session.commit()
#
#         # simulate that task runs
#         for i in range(task.sleep):
#             print('{}: Sleep, sec: {}'.format(uid, i))
#             time.sleep(1)
