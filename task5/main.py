from datetime import datetime, timedelta
import pandas as pd
import pymongo
from airflow import DAG
from airflow.operators.python import PythonOperator
import re

default_args = {
    'start_date': datetime(2022, 10, 3, hour=11, minute=14),
    'owner': 'airflow'
}
dag = DAG('pandas_to_mongo', default_args=default_args,
          schedule=timedelta(days=1))


def get_data(**kwargs):
    dataframe = pd.read_csv('/home/artem/airflow/dags/tiktok_google_play_reviews.csv')
    return dataframe.to_json()


def preprocessing(**kwargs):
    ti = kwargs['ti']
    dataframe = ti.xcom_pull(task_ids='get_data')
    dataframe = pd.read_json(dataframe)
    dataframe = delete_duplicates(dataframe)
    dataframe = replace_null_to_symbol(dataframe)
    dataframe = sort_by_date(dataframe)
    dataframe = remove_smiles(dataframe)
    return dataframe.to_json()


def delete_duplicates(dataframe):
    return dataframe.drop_duplicates()


def replace_null_to_symbol(dataframe):
    return dataframe.fillna('-')


def sort_by_date(dataframe):
    return dataframe.sort_values(by='at')


def remove_smiles(dataframe):
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               u"\U0001f926-\U0001f937"
                               u"\u200d"
                               u"\u2640-\u2642"
                               "]+", flags=re.UNICODE)

    dataframe.content = dataframe.content.replace(to_replace=emoji_pattern, value='', regex=True)
    return dataframe


def push_to_mongo(**kwargs):
    ti = kwargs['ti']
    client = pymongo.MongoClient(
        "mongodb+srv://bakhar:1111@cluster0.8pixcso.mongodb.net/?retryWrites=true&w=majority")
    db = client['tiktok']
    connection = db['airflow_tiktok']
    dataframe = ti.xcom_pull(task_ids='preprocessing')
    dataframe = pd.read_json(dataframe)
    dataframe.reset_index(inplace=True)
    dataframe_dict = dataframe.to_dict('records')
    connection.insert_many(dataframe_dict)


t1 = PythonOperator(task_id='get_data', dag=dag, python_callable=get_data)
t2 = PythonOperator(task_id='preprocessing', dag=dag, provide_context=True,
                    python_callable=preprocessing)
t3 = PythonOperator(task_id='push_mongo', dag=dag, provide_context=True, python_callable=push_to_mongo)
t1 >> t2 >> t3


