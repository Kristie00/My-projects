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

dag = DAG('de_exam_01', default_args=default_args)


def request_data():
    url = "https://restcountries.com/v3.1/all"
    r = requests.get(url)
    data = r.json()
    df = pandas.json_normalize(data)
    df3 = pandas.DataFrame(df)
    # print(df3.head())
    my_list = df3.columns.values.tolist()
    # print(my_list)
    cols = ['name.common', 'cca2', 'continents']
    df4 = df.loc[:, df.columns.isin(list(cols))]
    df5 = df4[['name.common', 'cca2', 'continents']]
    df6 = df5.explode('continents')
    # print(df5)
    return df6.values.tolist()


def transform(ti):
    data = ti.xcom_pull(task_ids='request_data')
    # print(data)
    df = pandas.DataFrame(data)
    # print(df)
    return df.values.tolist()


def send_to_mssql(ti):
    data = ti.xcom_pull(task_ids=['transform'])[0]
    hook = MsSqlHook(mssql_conn_id="mssql_conn")
    hook.insert_rows("exam.dbo.countries", data)
    return 0


request_data = PythonOperator(task_id='request_data', python_callable=request_data, dag=dag)
transform = PythonOperator(task_id='transform', python_callable=transform, dag=dag)
send_to_mssql = PythonOperator(task_id='send', python_callable=send_to_mssql, dag=dag)

request_data >> transform >> send_to_mssql