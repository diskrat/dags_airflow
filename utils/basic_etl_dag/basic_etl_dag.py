import pendulum
import os
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

conn_id_uri = os.getenv('AIRFLOW_CONN_LOPTESTE')
default_args = {
    'start_date': pendulum.naive(2024, 6, 8),
    'schedule_interval':None,
    'catchup':False,
}

@dag(
    dag_id='sql_query',
    default_args=default_args
)
def extract_sql():
    @task
    def extract():
        sqlquery="""
            SELECT id FROM lop2teste.submission
            WHERE createdAt  LIKE '{{ ds }}*' AND class_id = '{{params.class_id}}' AND listQuestions_id = '{{params.listQuestions_id}}'
            """
        
        class_id='8813d508-dc24-47e2-a33f-8d2ca66cab29'
        listQuestions_id = '1551e3ed-c875-4bb3-8e4e-58407df40776'
        query_task = SQLExecuteQueryOperator(
            task_id='execute_query',
            conn_id='my_db_conn',
            sql=sqlquery,
            params={
                'class_id':'8813d508-dc24-47e2-a33f-8d2ca66cab29',
                'listQuestions_id' : '1551e3ed-c875-4bb3-8e4e-58407df40776'
            }
        )
        print(query_task.execute(context={}))
    extract()

extract_sql()