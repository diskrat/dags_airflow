from airflow.models import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.mysql.hooks.mysql import MySqlHook
with DAG(
    dag_id="test_mysql_query_taskflow",
    schedule_interval=None,
    start_date=days_ago(2),
    catchup=False
    
) as dag:

    @task
    def test_mysql_query():
        # Create a MySqlHook instance
        # Ensure this matches your connection ID
        mysql_hook = MySqlHook(conn_id='mysql_default')

        # Define your SQL query
        sql_query = "SELECT * FROM submission WHERE class_id = '8813d508-dc24-47e2-a33f-8d2ca66cab29' AND listQuestions_id = '{{params.listQuestions_id}}'"

        # Get the DataFrame using get_pandas_df
        
        df = mysql_hook.get_pandas_df(sql_query)

        # Display the results
        print(df)

    # Calling the task
    test_mysql_query()
