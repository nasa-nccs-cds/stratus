from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


class BaseHandler:
    def create_dag(self, dag_id, resuest, default_args):

        def hello_world_py(*args):
            print('Hello World')
            print('This is DAG: {}'.format(str(dag_number)))

        dag = DAG( dag_id, default_args=default_args )
        with dag:
            t1 = PythonOperator( task_id='hello_world', python_callable=hello_world_py, dag_number=dag_number )

        globals()[dag_id] = dag
        return dag