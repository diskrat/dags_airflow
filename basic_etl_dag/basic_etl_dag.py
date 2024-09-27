import pendulum
import os
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.bash import BashOperator

conn_id_uri = os.getenv('AIRFLOW_CONN_LOPTESTE')
default_args = {
    'start_date': pendulum.naive(2024, 3, 25),
    'end_date': pendulum.naive(2024, 6, 8),
    'schedule_interval': None,
    'catchup': False,
}


@dag(
    dag_id='sql_query',
    default_args=default_args,
    params={
        'class_id': '8813d508-dc24-47e2-a33f-8d2ca66cab29',
        'listQuestions_id': '1551e3ed-c875-4bb3-8e4e-58407df40776'}
)
def extract_sql():
    @task
    def extract(logical_=None, **kwargs):
        sqlquery = """
            SELECT id FROM submission
            WHERE createdAt  LIKE '{{ ds }}*' AND class_id = '{{params.class_id}}' AND listQuestions_id = '{{params.listQuestions_id}}'
            """
        mysql_hook = MySqlHook(conn_id='mysql_default')
        with mysql_hook.get_conn()  as connection:
            cursor = connection.cursor()
            cursor.execute(sqlquery)
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=columns)
        filename = f"~/airflow/data/{logical_}.csv"
        df.to_csv(filename, index=False)
    bashtask = BashOperator(task_id='cria_pasta',
                            bash_command='mkdir -p ~/airflow/data/')
    save_extract = extract('{{ds}}')
    bashtask >> save_extract


extract_sql()
