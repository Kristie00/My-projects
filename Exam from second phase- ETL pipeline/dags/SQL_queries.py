from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils.dates import days_ago

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


dag = DAG('sql_queries', default_args=default_args, catchup=False)


def first():
    hook = MsSqlHook(mssql_conn_id='mssql_conn')
    sql = """
        SELECT TOP 10 WITH TIES name, elevation_ft
        FROM exam.dbo.airports
        ORDER BY elevation_ft DESC
        """
    return hook.get_records(sql)


def second():
    hook = MsSqlHook(mssql_conn_id='mssql_conn')
    sql = """
        SELECT a.name AS 'Airport', c.name AS 'Country', c.continent AS 'Continent'
        FROM exam.dbo.airports a
        LEFT JOIN exam.dbo.countries c 
        ON a.iso_country = c.iso2_code
        WHERE c.continent = 'Africa'
        """
    return hook.get_records(sql)

def third():
    hook = MsSqlHook(mssql_conn_id='mssql_conn')
    sql = """
        SELECT name, local_code
        FROM exam.dbo.airports
        WHERE local_code LIKE('02%')
        """
    return hook.get_records(sql)

def fourth():
    hook = MsSqlHook(mssql_conn_id='mssql_conn')
    sql = """
        SELECT MAX(a.elevation_ft), c.continent
        FROM exam.dbo.airports a
        LEFT JOIN exam.dbo.countries c 
        ON a.iso_country = c.iso2_code
        GROUP BY c.continent
        """
    return hook.get_records(sql)

def fifth():
    hook = MsSqlHook(mssql_conn_id='mssql_conn')
    sql = """
        SELECT COUNT(*)
        FROM exam.dbo.airports
        WHERE type = 'small_airport'
        """
    return hook.get_records(sql)

first_query = PythonOperator(task_id='first', python_callable=first, dag=dag)
second_query = PythonOperator(task_id='second', python_callable=second, dag=dag)
third_query = PythonOperator(task_id='third', python_callable=third, dag=dag)
fourth_query = PythonOperator(task_id='fourth',python_callable=fourth, dag=dag)
fifth_query = PythonOperator(task_id='fifth',python_callable=fifth, dag=dag)

first_query >> second_query >> third_query >> fourth_query >> fifth_query