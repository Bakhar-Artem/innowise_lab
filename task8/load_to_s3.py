from datetime import datetime
import boto3
import configparser
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read('s3.env')
AWS_REGION = config['AWS']['AWS_REGION']
ENDPOINT_URL = config['AWS']['ENDPOINT_URL']
BUCKET_NAME = config['AWS']['s3_bucket']
S3_KEY = config['AWS']['S3_KEY']
S3_SECRET = config['AWS']['S3_SECRET']

with DAG('s3', start_date=datetime.now()) as dag:
    def init_bucket():

        s3_client = boto3.client(
            service_name='s3',
            region_name=AWS_REGION,
            endpoint_url=ENDPOINT_URL,
            aws_access_key_id=S3_KEY,
            aws_secret_access_key=S3_SECRET,
            verify=False
        )
        response = s3_client.create_bucket(Bucket=BUCKET_NAME)


    def upload_files():
        s3_client = boto3.client(
            's3',
            region_name=AWS_REGION,
            endpoint_url=ENDPOINT_URL,
            aws_access_key_id=S3_KEY,
            aws_secret_access_key=S3_SECRET,
            verify=False
        )
        try:
            for txt_path in Path("result").glob("*.csv"):
                path = str(txt_path).split('/')
                s3_client.upload_file(str(txt_path), BUCKET_NAME, path[2])
        except ClientError as e:
            print(e.response)


    task1 = PythonOperator(task_id='init_bucket', python_callable=init_bucket)
    task2 = PythonOperator(task_id='upload_bucket', python_callable=upload_files)
    task1 >> task2
