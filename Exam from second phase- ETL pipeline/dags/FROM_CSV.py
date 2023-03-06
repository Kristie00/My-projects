from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'schedule_interval': '@weekly',
    'email': ['kri.kafkova@seznam.cz'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'catchup': False,
    'provide_context': True
}

dag = DAG('de_exam_02', default_args=default_args)


def extract():
    url = "https://pkgstore.datahub.io/core/airport-codes/airport-codes_json/data/9ca22195b4c64a562a0a8be8d133e700/airport-codes_json.json"
    get_data = requests.get(url)
    data = get_data.json()
    df = pandas.json_normalize(data)
    df2 = pandas.DataFrame(df)
    # my_list = df2.columns.values.tolist()
    # print(my_list)
    df.drop(columns=['continent'], inplace=True)
    df5 = df2[
        ['ident', 'type', 'name', 'elevation_ft', 'iso_country', 'iso_region', 'municipality', 'gps_code', 'iata_code',
         'local_code', 'coordinates']]
    return df5.values.tolist()


def transform(**context):
    data = (context['ti'].xcom_pull(task_ids='extract'))
    df = pandas.DataFrame(data,
                          columns=['ident', 'type', 'name', 'elevation_ft', 'iso_country', 'iso_region',
                                   'municipality', 'gps_code', 'iata_code', 'local_code', 'coordinates'])
    df.fillna('0', inplace=True)
    df.dropna(subset=['gps_code'], inplace=True)
    return df.values.tolist()


def load_to_mssql(**context):
    data = (context['ti'].xcom_pull(task_ids='transform'))
    hook = MsSqlHook(mssql_conn_id="mssql_conn")
    hook.insert_rows("exam.dbo.airports", data)
    return 0


extract = PythonOperator(task_id='extract', python_callable=extract, do_xcom_push=True, dag=dag)
transform = PythonOperator(task_id='transform', python_callable=transform, do_xcom_push=True, dag=dag)
load = PythonOperator(task_id='load', python_callable=load_to_mssql, dag=dag)

extract >> transform >> load
