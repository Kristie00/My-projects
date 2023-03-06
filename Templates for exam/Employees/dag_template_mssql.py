from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
from airflow.utils.dates import days_ago


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

dag = DAG('etl_practice', default_args=default_args, catchup=False)

# fetch data from MSSQL, transform it and send it to different table in MSSQL

def _get_data_from_mssql():
    hook = MsSqlHook(mssql_conn_id='mssql_conn')
    sql = """
        SELECT id, first_name, last_name, gender
        FROM AirflowSet.dbo.employee
        """
    return hook.get_records(sql)

def _transform(ti):
    data = ti.xcom_pull(task_ids=['extract'])[0]
    df = pd.DataFrame(data, columns=['id', 'first_name', 'last_name', 'gender'])
    df = df.loc[df['gender'] == 'male']
    df['first_name'] = df['first_name'].replace(['John'], 'Paul')
    df['name'] = df['first_name'] + ' ' + df['last_name']
    print(df['name'])
    return df.values.tolist()

def _load(ti):
    data = ti.xcom_pull(task_ids=['transform'])[0]
    df = pd.DataFrame(data, columns=['id', 'first_name', 'last_name', 'gender', 'name'])
    # del df['id']
    # del df['gender']
    # df = df[['id', 'first_name', 'last_name', 'gender', 'name']]
    data = df.values.tolist()
    hook = MsSqlHook(mssql_conn_id='mssql_conn')
    hook.insert_rows('AirflowSet.dbo.transformed', data)
    return 0

def _starting():
    return "Start of the work-flow!"

def _ending():
    return "End of the work-flow!"

extract = PythonOperator(task_id='extract', python_callable=_get_data_from_mssql, dag=dag)
transform = PythonOperator(task_id='transform', python_callable=_transform, dag=dag)
load = PythonOperator(task_id='load', python_callable=_load, dag=dag)
starting_task = PythonOperator(task_id="start",python_callable=_starting, dag=dag)
ending_task = PythonOperator(task_id="end",python_callable=_ending, dag=dag)

[extract, starting_task] >> transform >> [load, ending_task]