import pendulum
import os
import json
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.bash import BashOperator
from sqlalchemy import create_engine
from sqlalchemy import text

conn_id_uri = os.getenv('AIRFLOW_CONN_LOPTESTE')
default_args = {
    'start_date': pendulum.datetime(2024, 3, 25).in_tz('America/Recife'),
    'end_date': pendulum.datetime(2024, 6, 15).in_tz('America/Recife'),
}


@dag(
    dag_id='sql_to_mongo',
    default_args=default_args,
    params={
        'class_id': '8813d508-dc24-47e2-a33f-8d2ca66cab29'}
)
def extract_sql():
    
    

    @task
    def extract(date, class_id):
        directory_raw = "/home/apairflow/airflow/raw_data"
        os.makedirs(directory_raw, exist_ok=True)
        sqlquery = f"SELECT id,listQuestions_id FROM submission WHERE DATE(createdAt) ='{date}' AND class_id = '{class_id}';"
        print(sqlquery)
        mysql_hook = MySqlHook(conn_id='mysql_default')
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sqlquery)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result, columns=columns)

        filename = f"/home/apairflow/airflow/raw_data/{date}.csv"
        try:
            df.to_csv(filename, index=False)
            print(f"File '{filename}' created successfully.")

        except Exception as e:
            print(f"An error occurred: {e}")

    @task
    def transform(date):
        directory_transformed = "/home/apairflow/airflow/transformed_data"
        os.makedirs(directory_transformed, exist_ok=True)
        raw_filename = f'/home/apairflow/airflow/raw_data/{date}.csv'
        data = pd.read_csv(raw_filename)
        result = data["listQuestions_id"].value_counts().to_dict()
        print(result)
        transformed_filename = f"/home/apairflow/airflow/transformed_data/{date}.json"
        with open(transformed_filename, 'x') as json_file:
            json.dump(result, json_file)
        os.remove(raw_filename)

    @task
    def load(date):
        hook = MongoHook(conn_id='mongo_retry')
        client = hook.get_conn()
        db = client.acessos
        diarios = db.diarios
        date_formated = pendulum.parse(date).in_tz('America/Recife')
        filename = f"/home/apairflow/airflow/transformed_data/{date}.json"
        with open(filename, 'r') as json_file:
            transformed_data = json.load(json_file)
        diarios.update_one({
            "_id": date_formated},
            {"$set": transformed_data},
            upsert=True)
        os.remove(f'/home/apairflow/airflow/transformed_data/{date}.json')

    t1 = extract('{{ds}}', '{{params.class_id}}')
    t2 = transform('{{ds}}')
    t3 = load('{{ds}}')
    t1 >> t2 >> t3


extract_sql()
