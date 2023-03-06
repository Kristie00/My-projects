from datetime import datetime
import json
import requests
from airflow import DAG
import pandas

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 15),
    'email': ['kri.kafkova@seznam.cz'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0
}


def extract_rest():
    url = "http://127.0.0.1:5000/api/products/products?type="
    querystring = {"key1": "Sporstwear", "key2": "Clothing", "key3": "Various"}

    response = requests.request("GET", url, params=querystring)
    response_json = json.loads(response.text)
    result = {}
    for i in range(len(response_json)):
        result.update(i)
    print(result)
    return result


def extract_file():
    df_dict = {}
    fileCSV = pandas.read_csv("https://raw.githubusercontent.com/woocommerce/woocommerce/master/sample-data/sample_products.csv")
    df = pandas.DataFrame(fileCSV)
    for i in df.itertuples():
        if i[4] == 1:
            df[i].to_dict()
            df_dict.update(i)
        else:
            pass
    print(df_dict)
    return df


def transform(file: dict):
    transform_dict = {}
    for k, v in file.items():
        if k == "Name":
            transform_dict.update({k: v})
        elif k == "SKU":
            transform_dict.update({k: v})
        elif k == "Regular price":
            transform_dict.update({k: v})
        elif k == "Categories":
            transform_dict.update({"Type": v})
        elif k == "Is featured?":
            transform_dict.update({"Featured": v})
        elif k == "Short description":
            transform_dict.update({"Description": v})
        else:
            pass
    print(transform_dict)
    return transform_dict

# with DAG(dag_id="rest_dag", default_args=default_args, schedule_interval='0 4 * * 1-5', catchup=False) as d:
