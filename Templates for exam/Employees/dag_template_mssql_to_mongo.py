from airflow import DAG
from airflow.operators.python import PythonOperator
import json
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
from airflow.utils.dates import days_ago
from pymongo import MongoClient


default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'start_date': days_ago(1),
   'email': ['your-email@email.com'],
   'email_on_failure': True,
   'email_on_retry': False,
   'retries': 0,
   'catchup': False,
}

dag = DAG('etl_mongo', default_args=default_args, catchup=False)

# fetch data from MSSQL, transform it and send it into MongoDB

def _get_data_from_mssql():
    hook = MsSqlHook(mssql_conn_id='mssql_conn')
    sql = """
        SELECT id, first_name, last_name, gender, nationality, salary
        FROM AirflowSet.dbo.employee
        """
    return hook.get_records(sql)


def _transform(**context):
    # data = ti.xcom_pull(task_ids=['extract'])[0]
    data = (context['ti'].xcom_pull(task_ids='extract'))
    df = pd.DataFrame(data, columns=['id', 'first_name', 'last_name', 'gender', 'nationality', 'salary'])
    result = df.to_json(orient="index", date_format="iso")
    parsed = json.loads(result)
    data2 = json.dumps(parsed, indent=4)
    return data2


def _load(ti):
    data = ti.xcom_pull(task_ids=['transform'])[0]
    data4 = json.loads(data)
    cluster = MongoClient("mongodb+srv://*:*@cluster0.abrrsjb.mongodb.net/?retryWrites=true&w=majority")
    db = cluster["myfirstdb"]
    collection = db["names"]
    collection.insert_many([data4])
    return 0


extract = PythonOperator(task_id='extract', python_callable=_get_data_from_mssql, dag=dag)
transform = PythonOperator(task_id='transform', python_callable=_transform, dag=dag)
load = PythonOperator(task_id='load', python_callable=_load, dag=dag)

extract >> transform >> load

