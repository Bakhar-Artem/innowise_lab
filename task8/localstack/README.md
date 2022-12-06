# This folder describes used localstack services
## The folder consists of prebuild *pandas==1.5.2* and custom files
## Custom files:
1) *lambda_handler.py* describes lambda handler and extra methods(count metrics, get files from s3 and put to DynamoDb)
2) *localstack_init.py* describes used localstack services and call init methods for them
3) *iam.json* example of iam aws for the lambda
4) *s3_notification.json* exam for s3 notification to sqs
5) *README.md*

# Used Localstack services:
1) S3
2) SQS
3) Lambda function
4) IAM
5) DynamoDb
# Run
To run all needed services start your docker container and then execute *localstack_init.py*
# Extra info
Lambda function start on Python3.8 