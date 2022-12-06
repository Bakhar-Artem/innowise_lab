import configparser
import logging
import shutil
import boto3

# get credits to aws(localstack)
config = configparser.ConfigParser()
config.read('./s3.env')
AWS_REGION = config['AWS']['AWS_REGION']
ENDPOINT_URL = config['AWS']['ENDPOINT_URL']
BUCKET_NAME = config['AWS']['BUCKET_NAME']
S3_KEY = config['AWS']['S3_KEY']
S3_SECRET = config['AWS']['S3_SECRET']
LAMBDA_ZIP = '../function'

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)s: %(message)s')


def get_boto3_client(service):
    """
    Initialize Boto3 Lambda client.
    """
    try:
        lambda_client = boto3.client(
            service,
            region_name=AWS_REGION,
            endpoint_url=ENDPOINT_URL,
            aws_access_key_id=S3_KEY,
            aws_secret_access_key=S3_SECRET,
            verify=False
        )
    except Exception as e:
        logger.exception('Error while connecting to LocalStack.')
        raise e
    else:
        return lambda_client


def create_lambda_zip():
    """
    Generate ZIP file for localstack function.
    """
    try:
        shutil.make_archive(LAMBDA_ZIP, 'zip', './')
    except Exception as e:
        logger.exception('Error while creating ZIP file.')
        raise e


def create_lambda(function_name):
    """
    Creates a Lambda function in LocalStack and attach iam role
    """
    try:
        lambda_client = get_boto3_client('lambda')
        _ = create_lambda_zip()

        # create zip file for localstack function.
        with open(LAMBDA_ZIP + '.zip', 'rb') as f:
            zipped_code = f.read()

        lambda_client.create_function(
            FunctionName=function_name,
            Runtime='python3.8',
            Role='arn:aws:iam::000000000000:role/localstack-s3-role',
            Handler=function_name + '.handler',
            Code=dict(ZipFile=zipped_code)
        )
    except Exception as e:
        logger.exception('Error while creating function.')
        raise e


def create_bucket():
    """
    Initializes s3 bucket with name BUCKET_NAME from s3.env
    :return:
    """
    s3_client = get_boto3_client('s3')
    s3_client.create_bucket(Bucket=BUCKET_NAME)


def create_role():
    """
    Creates iam role for the function execution
    :return:
    """
    iam_client = get_boto3_client('iam')

    resp1 = iam_client.create_policy(PolicyName='my-pol',
                                     PolicyDocument="""{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:PutLogEvents",
        "logs:CreateLogGroup",
        "logs:CreateLogStream"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject"
      ],
      "Resource": "arn:aws:s3:::results/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes",
        "sqs:ReceiveMessage"
      ],
      "Resource": "arn:aws:sqs:us-east-1:000000000000:check_notify"
    },
    {
      "Effect": "Allow",
			"Action": [
				"dynamodb:BatchGetItem",
				"dynamodb:GetItem",
				"dynamodb:Query",
				"dynamodb:Scan",
				"dynamodb:BatchWriteItem",
				"dynamodb:PutItem",
				"dynamodb:UpdateItem"
			],
			"Resource": "arn:aws:dynamodb:us-east-1:000000000000:table/*"
    }
  ]
}""")
    resp2 = iam_client.create_role(
        RoleName='localstack-s3-role',
        AssumeRolePolicyDocument="""{"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": {"Service": "localstack.amazonaws.com"}, "Action": "sts:AssumeRole"}]}""")
    resp3 = iam_client.attach_role_policy(
        RoleName='localstack-s3-role',
        PolicyArn='arn:aws:iam::000000000000:policy/my-pol')


def create_notification():
    """
    Creates notification for s3 bucket to sqs
    :return:
    """
    s3_client = get_boto3_client('s3')
    s3_client.put_bucket_notification_configuration(
        Bucket=BUCKET_NAME,
        NotificationConfiguration={
            "QueueConfigurations": [
                {
                    "QueueArn": "arn:aws:sqs:us-east-1:000000000000:check_notify",
                    "Events": [
                        "s3:ObjectCreated:*"
                    ]
                }
            ]
        }, )


def create_event_mapping():
    """
    Creates event mapping to get messages from sqs to lambda
    :return:
    """
    lambda_client = get_boto3_client('lambda')
    lambda_client.create_event_source_mapping(
        EventSourceArn='arn:aws:sqs:us-east-1:000000000000:check_notify',
        FunctionName='lambda_handler',
        Enabled=True,
        BatchSize=3, )


def create_sqs():
    """
    Creates sqs
    :return:
    """

    sqs_client = get_boto3_client('sqs')
    resp = sqs_client.create_queue(QueueName='check_notify', )


def delete_function():
    """
    Extra method in case to recreate lambda function
    :return:
    """

    lambda_f = get_boto3_client('lambda')
    lambda_f.delete_function(FunctionName='lambda_handler')


def create_raw_table():
    """
    Creates DynamoDb table for raw data
    :return:
    """

    dynamodb_client = get_boto3_client('dynamodb')
    dynamodb_client.create_table(
        AttributeDefinitions=[
            {
                'AttributeName': 'year',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'month',
                'AttributeType': 'N'
            }
        ],
        TableName='raw_data',
        KeySchema=[
            {
                'AttributeName': 'year',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'month',
                'KeyType': 'RANGE'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        },
    )


def create_avg_table():
    """
    Creates DynamoDb table for the counted metrics
    :return:
    """
    dynamodb_client = get_boto3_client('dynamodb')
    dynamodb_client.create_table(
        AttributeDefinitions=[
            {
                'AttributeName': 'year',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'month',
                'AttributeType': 'N'
            }
        ],
        TableName='avg',
        KeySchema=[
            {
                'AttributeName': 'year',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'month',
                'KeyType': 'RANGE'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        },
    )


def init_all():
    """
    Call all init methods to init localstack services
    :return:
    """

    create_sqs()
    create_role()
    create_bucket()
    create_lambda('lambda_handler')
    create_notification()
    create_event_mapping()
    create_raw_table()
    create_avg_table()


if __name__ == '__main__':
    init_all()

# in case to recreate localstack function
# delete_function()
# create_lambda('lambda_handler')
# create_event_mapping()
