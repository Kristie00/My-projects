from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
import requests
import pandas
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'schedule_interval': '@weekly',
    'email': ['your-email@email.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'catchup': False,
    'provide_context': True
}

dag = DAG('rest_api_to_sql', default_args=default_args)

# Fetch data from URL and send them to MSSQL

def request_data():
    url = "https://catfact.ninja/fact"
    r = requests.get(url)
    data = r.json()

    # If you want to write data into file, use the code inside comment.
    # If the file does not exist, it will be created automatically.

    # json_object = json.dumps(data)
    #
    # my_path = os.path.abspath(os.path.dirname(__file__))
    # path = os.path.join(my_path, "cats.json")
    #
    # with open(path, "w") as outfile:
    #     outfile.write(json_object)
    #
    # with open(path) as project_file:
    #     data = json.load(project_file)

    df = pandas.json_normalize(data)
    df3 = pandas.DataFrame(df)

    return df3.values.tolist()


def transform(ti):
    data = ti.xcom_pull(task_ids='request_data')
    print(data)
    df = pandas.DataFrame(data, columns=["fact", "length"])
    print(df)
    df['length'] = df['length'].astype(int)
    return df.values.tolist()



def send_to_mssql(ti):
    data = ti.xcom_pull(task_ids=['transform'])[0]
    # You can convert data from xcom to dataframe and make some changes.
    # Then you have to convert the dataframe values into list:
    # data2 = pandas.DataFrame(data)
    # result = data2.values.tolist()
    hook = MsSqlHook(mssql_conn_id="mssql_conn")
    hook.insert_rows("cats.dbo.cats", data)
    return 0


request_data = PythonOperator(task_id='request_data', python_callable=request_data, dag=dag)
transform = PythonOperator(task_id='transform', python_callable=transform, dag=dag)
send_to_mssql = PythonOperator(task_id='send', python_callable=send_to_mssql, dag=dag)

request_data >> transform >> send_to_mssql
