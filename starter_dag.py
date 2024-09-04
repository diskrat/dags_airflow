from airflow.models import DAG
import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

with DAG(
    'extra_work',
    start_date=pendulum.today('America/Recife').add(days=-2),
    schedule_interval='@daily'
) as dag:
    tarefa_1 = EmptyOperator(task_id='tarefa1')
    tarefa_2 = EmptyOperator(task_id='tarefa2')
    tarefa_3 = EmptyOperator(task_id='tarefa3')
    tarefa_4 = BashOperator(
        task_id='cria_pasta',
        bash_command='mkdir -p /home/joao/docs/airflow/pasta{{data_interval_end}}'
    )
    
    tarefa_1 >> [tarefa_2,tarefa_3]
    tarefa_3 >> tarefa_4