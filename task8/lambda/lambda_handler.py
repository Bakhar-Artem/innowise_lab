import logging
import json
from localstack_init import get_boto3_client, BUCKET_NAME

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def get_key(event):
    body = event['Records'][0]['body']
    body = '{' + body[1:]
    body_json = json.loads(body)
    return str(body_json['Records'][0]['s3']['object']['key'])


def get_prefix(key):
    prefix = str(key).split('/')
    return prefix[0]


def handler(event, context):
    logging.info('check behavior')
    key = get_key(event)
    prefix = get_prefix(key)
    resp = get_boto3_client('s3').list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    if resp['KeyCount'] != 3:
        return {
            "message": "Wait for all files"
        }
    else:
        with open('/logs.txt', 'w') as f:
            f.write('success')
    return {
        "message": "success"
    }
