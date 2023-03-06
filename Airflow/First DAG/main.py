from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable

default_args = {
    'start_date': datetime(2022, 11, 15),
}


def hello():
    return "Hello"


def show_variable():
    dag_config = Variable.get("hello_text", deserialize_json=True)
    var1 = dag_config["hello_text"]
    return var1


with DAG('initial', default_args=default_args, schedule_interval="0 14 * * 1-5") as dag:
    say_hello = PythonOperator(
        task_id="say_hello",
        python_callable=hello,
    )

    use_variable = PythonOperator(
        task_id="use_variable",
        python_callable=show_variable
    )

say_hello >> use_variable
