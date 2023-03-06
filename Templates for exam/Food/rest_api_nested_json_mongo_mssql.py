import pandas as pdfrom airflow import DAG
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
# This is also called api, but we read the data from file LOL, not from URL
# Read data from file (quite nested json), transform it and them to:
# #   1. MSSQL- data is from XCOM
# #   2. MongoDB- data is from MSSQL table - this might not work at first, because there will be no data in that table
# #   3. MongoDB- data is from XCOM

dag = DAG('food_api_etl', default_args=default_args, catchup=False)
def _get_data_from_api():
    my_path = os.path.abspath(os.path.dirname(__file__))
    path = os.path.join(my_path, "food.json")
    with open(path) as project_file:
        data = json.load(project_file)
    df = pandas.json_normalize(data, 'categories')
    df3 = pandas.DataFrame(df)
    return df3.values.tolist()


def _transform(ti):
    data1 = ti.xcom_pull(task_ids=['extract'])[0]
    df = pandas.DataFrame(data1,
                          columns=["idCategory", "strCategory", "strCategoryThumb", "strCategoryDescription"])
    return df.values.tolist()


def _load_mssql_table(ti):
    data = ti.xcom_pull(task_ids=['transform'])[0]
    hook = MsSqlHook(mssql_conn_id="mssql_conn")
    hook.insert_rows("cats.dbo.people", data)
    return 0

def _load_mongodb(ti):
    hook = MsSqlHook(mssql_conn_id="mssql_conn")
    sql = """
           SELECT ID, Category, URL, Description           
           FROM cats.dbo.people
       """
    df = hook.get_pandas_df(sql)
    cluster = MongoClient("mongodb+srv://*:*@cluster0.abrrsjb.mongodb.net/?retryWrites=true&w=majority")
    db = cluster["myfirstdb"]
    collection = db["food"]
    collection.insert_many(df.to_dict('records'))
    return 0

def _load_mongodb_from_xcom(ti):
    data = ti.xcom_pull(task_ids=['transform'])[0]
    df = pandas.DataFrame(data,
                          columns=["idCategory", "strCategory", "strCategoryThumb", "strCategoryDescription"])
    cluster = MongoClient("mongodb+srv://*:*@cluster0.abrrsjb.mongodb.net/?retryWrites=true&w=majority")
    db = cluster["myfirstdb"]
    collection = db["people"]
    collection.insert_many(df.to_dict('records'))
    return 0

extract = PythonOperator(task_id='extract', python_callable=_get_data_from_api, dag=dag)
transform = PythonOperator(task_id='transform', python_callable=_transform, dag=dag)
load_mssql = PythonOperator(task_id='load_mssql', python_callable=_load_mssql_table, dag=dag)
load_mongo = PythonOperator(task_id='load_mongo', python_callable=_load_mongodb, dag=dag)
load_mongo_from_xcom = PythonOperator(task_id='load_mongo_from_xcom', python_callable=_load_mongodb_from_xcom, dag=dag)

extract >> transform >> load_mssql >> [load_mongo, load_mongo_from_xcom]

