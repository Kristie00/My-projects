from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
from pymongo import MongoClient

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 5),
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': True,
    'schedule_interval': '@weekly'
}

dag = DAG('de_exam_03', default_args=default_args, catchup=False)


# Retrieve
#       name, type, elevation_ft, iso_region, gps_code, local_code of airport
#       and name, continent from countries
#       and insert the result to MongoDB collection airports as separate documents.

def get_data_from_mssql():
    hook = MsSqlHook(mssql_conn_id='mssql_conn')
    sql = """
        SELECT a.name, a.type, a.elevation_ft, a.iso_region, a.gps_code, a.local_code, c.name, c.continent
        FROM exam.dbo.airports a
        LEFT JOIN exam.dbo.countries c
        ON a.iso_country = c.iso2_code
        """
    return hook.get_records(sql)


def transform(**context):
    data = (context['ti'].xcom_pull(task_ids='extract'))
    df = pd.DataFrame(data, columns=['name of airport', 'type', 'elevation_ft', 'iso_region', 'gps_code',
                                     'local_code', 'name of country', 'continent'])
    result = df.to_json(orient="index")
    parsed = json.loads(result)
    data2 = json.dumps(parsed, indent=4)
    return data2


def load(ti):
    data = ti.xcom_pull(task_ids=['transform'])[0]
    data4 = json.loads(data)
    cluster = MongoClient("mongodb+srv://kristyna:kristyna@cluster0.abrrsjb.mongodb.net/?retryWrites=true&w=majority")
    db = cluster["myfirstdb"]
    collection = db["airport"]
    collection.insert_many([data4])
    return 0


extract = PythonOperator(task_id='extract', python_callable=get_data_from_mssql, dag=dag)
transform = PythonOperator(task_id='transform', python_callable=transform, dag=dag)
load = PythonOperator(task_id='load', python_callable=load, dag=dag)

extract >> transform >> load
