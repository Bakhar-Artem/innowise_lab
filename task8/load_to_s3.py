from datetime import datetime
from io import StringIO

import boto3
import configparser
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.utils.task_group import TaskGroup
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession

# Credits to connect to aws(localstack)
config = configparser.ConfigParser()
config.read('/home/user/airflow/dags/s3.env')
AWS_REGION = config['AWS']['AWS_REGION']
ENDPOINT_URL = config['AWS']['ENDPOINT_URL']
BUCKET_NAME = config['AWS']['BUCKET_NAME']
S3_KEY = config['AWS']['S3_KEY']
S3_SECRET = config['AWS']['S3_SECRET']

with DAG('s3', start_date=datetime(2022, 11, 20)) as dag:
    def get_files_path():
        """
        method to get full csv paths
        :return: list of csv paths
        """
        paths = []
        for csv_path in Path("/home/user/airflow/dags/result").glob("*.csv"):
            paths.append(csv_path)
        return paths


    with TaskGroup('upload') as upload_task:
        # task group for uploads task
        def upload_files_to_s3():
            """
            method to upload csv files to s3
            :return: none
            """
            s3_client = boto3.client(
                's3',
                region_name=AWS_REGION,
                endpoint_url=ENDPOINT_URL,
                aws_access_key_id=S3_KEY,
                aws_secret_access_key=S3_SECRET,
                verify=False)
            for csv_path in get_files_path():
                try:
                    path = str(csv_path).split('/')
                    file_prefix = path[6].split('.')[0]
                    s3_client.upload_file(str(csv_path), BUCKET_NAME, file_prefix + '/main.csv')
                except ClientError as e:
                    print(e.response)


        def get_sc():
            """
            method to get connection to spark cluster
            :return: SparkSession
            """
            sc = SparkSession.builder.master('spark://localhost:7077').appName('test').config('spark.executor.memory',
                                                                                              '4g').config(
                'spark.executor.cores',
                '3').getOrCreate()
            return sc


        def get_s3_cred():
            """
            method to get connection to s3 bucket
            :return: session resource
            """
            session = boto3.Session(
                region_name=AWS_REGION,
                aws_access_key_id=S3_KEY,
                aws_secret_access_key=S3_KEY
            )
            s3 = session.resource(service_name='s3', endpoint_url=ENDPOINT_URL)
            return s3


        def upload_departure_to_s3():
            """
            method to count and upload departure metrics
            :return: none
            """
            sc = get_sc()
            for csv_path in get_files_path():
                df = sc.read.option('header', True).csv(str(csv_path))
                df = df.groupby('departure_name').count()

                csv_buffer = StringIO()
                df.toPandas().to_csv(csv_buffer, index=False)

                path = str(csv_path).split('/')
                file_prefix = path[6].split('.')[0]
                get_s3_cred().Object(BUCKET_NAME, file_prefix + '/departure.csv').put(Body=csv_buffer.getvalue())


        def upload_return_to_s3():
            """
            method to count and upload return metrics
            :return: none
            """
            sc = get_sc()

            for csv_path in get_files_path():
                df = sc.read.option('header', True).csv(str(csv_path))
                df = df.groupby('return_name').count()
                csv_buffer = StringIO()
                df.toPandas().to_csv(csv_buffer, index=False)
                path = str(csv_path).split('/')
                file_prefix = path[6].split('.')[0]
                get_s3_cred().Object(BUCKET_NAME, file_prefix + '/return.csv').put(Body=csv_buffer.getvalue())


        # PythonOperators for TaskGroup
        upload_files_task = PythonOperator(task_id='upload_files_to_s3', python_callable=upload_files_to_s3)
        upload_files_departure_task = PythonOperator(task_id='upload_metrics_departure',
                                                     python_callable=upload_departure_to_s3)
        upload_files_return_task = PythonOperator(task_id='upload_metrics_return', python_callable=upload_return_to_s3)
