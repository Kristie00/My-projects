from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils.dates import days_ago
from pymongo import MongoClient

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'schedule_interval': '@weekly',
    'email_on_retry': False,
    'retries': 0,
    'catchup': False,
}

dag = DAG('MONGO_QUERIES', default_args=default_args, catchup=False)


def first():
    client = MongoClient("mongodb+srv://kristyna:kristyna@cluster0.abrrsjb.mongodb.net/?retryWrites=true&w=majority")
    db = client["myfirstdb"]
    coll = db["airport"]
    result = coll.find({}, {"name": 1, "continent": 1, "_id": 0}).sort({"elevation_ft": -1})
    return list(result)


def second():
    client = MongoClient("mongodb+srv://kristyna:kristyna@cluster0.abrrsjb.mongodb.net/?retryWrites=true&w=majority")
    db = client["myfirstdb"]
    coll = db["airport"]
    result = coll.find({"type": "heliport"}, {"name": 1}).count()
    return list(result)


first_query = PythonOperator(task_id='first', python_callable=first, dag=dag)
second_query = PythonOperator(task_id='second', python_callable=second, dag=dag)

first_query >> second_query
