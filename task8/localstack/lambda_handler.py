import logging
import json
import pandas as pd
from localstack_init import get_boto3_client, BUCKET_NAME

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def get_key(event):
    """
    extra method to parse event and get the key of the uploaded file
    :param event:
    :return: s3 key for file
    """
    body = event['Records'][0]['body']
    body = '{' + body[1:]
    body_json = json.loads(body)
    return str(body_json['Records'][0]['s3']['object']['key'])


def get_prefix(key):
    """
    extra method to parse key and get specified prefix in next format: month-year
    :param key: s3 key for file
    :return: string m-year
    """
    prefix = str(key).split('/')
    return prefix[0]


def put_raw_data(prefix):
    """
    method to upload raw data in DynamoDb
    :param prefix:
    :return: None
    """
    dynamodb_client = get_boto3_client('dynamodb')
    month_year = prefix.split('-')
    dynamodb_client.put_item(TableName='raw_data', Item={
        'year': {
            'N': month_year[1]
        },
        'month': {
            'N': month_year[0]
        },
        'main': {
            'S': f's3://{BUCKET_NAME}/{prefix}/main.csv'
        },
        'return': {
            'S': f's3://{BUCKET_NAME}/{prefix}/return.csv'
        },
        'departure': {
            'S': f's3://{BUCKET_NAME}/{prefix}/departure.csv'
        }
    })


def put_avg(prefix, avgs):
    """
    method to upload counted metrics for file to DynamoDb
    :param prefix: string
    :param avgs: list[avg_distance, avg_duration, avg_speed,avg_temperature]
    :return: None
    """
    dynamodb_client = get_boto3_client('dynamodb')
    month_year = prefix.split('-')
    dynamodb_client.put_item(TableName='avg', Item={
        'year': {
            'N': month_year[1]
        },
        'month': {
            'N': month_year[0]
        },
        'distance day(m)': {
            'S': f'{str(avgs[0])}'
        },
        'duration day': {
            'S': f'{str(avgs[1])}'
        },
        'speed day': {
            'S': f'{str(avgs[2])}'
        },
        'temperature': {
            'S': f'{str(avgs[3])}'
        }
    })


def get_main_dataframe(prefix):
    """
    method to read s3 file to pandas
    :param prefix:
    :return: pandas dataframe
    """
    responce = get_boto3_client('s3').get_object(Bucket=BUCKET_NAME, Key=prefix + '/main.csv')
    df = pd.read_csv(responce.get('Body'))
    return df


def count_avg_distance(dataframe):
    """
    method to count avg_distance
    :param dataframe: read s3 file
    :return: avg_distance
    """
    return dataframe['distance (m)'].sum() / 30


def count_avg_duration(dataframe):
    """
    method to count avg_duration
    :param dataframe: read s3 file
    :return: avg_duration
    """
    return dataframe['duration (sec.)'].sum() / 30


def count_avg_speed(dataframe):
    """
    method to count avg_speed
    :param dataframe: read s3 file
    :return: avg_speed
    """
    return dataframe['avg_speed (km/h)'].sum() / 30


def count_avg_temperature(dataframe):
    """
    method to count avg_temperature
    :param dataframe: read s3 file
    :return: avg_temperature
    """
    return dataframe['Air temperature (degC)'].sum() / 30


def count_avg(prefix):
    """
    method to call all metric methods and collect them to list
    :param prefix:
    :return: list[avg_distance, avg_duration, avg_speed,avg_temperature]
    """
    dataframe = get_main_dataframe(prefix)
    avgs = [count_avg_distance(dataframe), count_avg_duration(dataframe), count_avg_speed(dataframe),
            count_avg_temperature(dataframe)]
    return avgs


def handler(event, context):
    """
    Lambda handler
    :param event:
    :param context:
    :return: log message
    """
    logging.info('check behavior')
    key = get_key(event)
    prefix = get_prefix(key)
    resp = get_boto3_client('s3').list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    if resp['KeyCount'] == 3:
        put_raw_data(prefix)
        put_avg(prefix, count_avg(prefix))
    else:
        return {
            "message": "Waiting for all files"
        }
    return {
        "message": "success"
    }
