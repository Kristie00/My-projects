from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta, date
import json
import os
import pandas
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from pymongo import MongoClient

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 23),
    'schedule_interval': '@weekly',
    'retries': 0,
    'provide_context': True
}

dag = DAG('emplolyee_api_etl', default_args=default_args, catchup=False)

# This is called api, but we read the data from file LOL, not from URL
# read data from file (not that nested json), transform it and then to:
#   1. MSSQL- data is from XCOM
#   2. MongoDB- data is from MSSQL table - this might not work at first, because there will be no data in that table
#   3. MongoDB- data is from XCOM

def _get_data_from_api():
    my_path = os.path.abspath(os.path.dirname(__file__))
    path = os.path.join(my_path, "employees.json")
    with open(path) as project_file:
        data = json.load(project_file)
    df = pandas.json_normalize(data)
    df3 = pandas.DataFrame(df)
    return df3.values.tolist()


def _transform(ti):
    data = ti.xcom_pull(task_ids=['extract'])[0]
    df = pandas.DataFrame(data,
                          columns=["id", "name", "birth_date", "nationality", "gender", "monthly_salary", "university"])
    df = df.loc[df["gender"] == 'Female']
    pandas.to_datetime(df['birth_date'])
    df.drop(["id"], inplace=True, axis=1)
    df["monthly_salary"] = df["monthly_salary"].astype(int)
    return df.values.tolist()


def _load_mssql_table(ti):
    data = ti.xcom_pull(task_ids=['transform'])[0]
    hook = MsSqlHook(mssql_conn_id="mssql_conn")
    hook.insert_rows("cats.dbo.people2", data)
    return 0


def _load_mongodb(ti):
    hook = MsSqlHook(mssql_conn_id="mssql_conn")
    sql = """
           SELECT id,name,nationality,gender,monthly_salary,university
           FROM cats.dbo.people2
       """
    df = hook.get_pandas_df(sql)
    cluster = MongoClient("mongodb+srv://*:*@cluster0.abrrsjb.mongodb.net/?retryWrites=true&w=majority")
    db = cluster["myfirstdb"]
    collection = db["food2"]
    collection.insert_many(df.to_dict('records'))
    return 0


def _load_mongodb_from_xcom(ti):
    data = ti.xcom_pull(task_ids=['transform'])[0]
    df = pandas.DataFrame(data, columns=["name", "birth_date", "nationality", "gender", "monthly_salary", "university"])
    data = df.values.tolist()
    cluster = MongoClient("mongodb+srv://*:*@cluster0.abrrsjb.mongodb.net/?retryWrites=true&w=majority")
    db = cluster["myfirstdb"]
    collection = db["people2"]
    collection.insert_many(df.to_dict('records'))
    return 0


extract = PythonOperator(task_id='extract', python_callable=_get_data_from_api, dag=dag)
transform = PythonOperator(task_id='transform', python_callable=_transform, dag=dag)
load_mssql = PythonOperator(task_id='load_mssql', python_callable=_load_mssql_table, dag=dag)
load_mongo = PythonOperator(task_id='load_mongo', python_callable=_load_mongodb, dag=dag)
load_mongo_from_xcom = PythonOperator(task_id='load_mongo_from_xcom', python_callable=_load_mongodb_from_xcom, dag=dag)

extract >> transform >> load_mssql >> [load_mongo, load_mongo_from_xcom]
