"""
etl py script on aws glue https://youtu.be/EQ44K5GfgDw?t=3044

see first edition: 07_showflake-etl-aws-glue.py
In this version I added secret passing from secrets manager
"""

# https://eu-north-1.console.aws.amazon.com/gluestudio/home?region=eu-north-1#/editor/job/sf-connector/script

import sys
import os
import json
import snowflake.connector
import boto3

from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions


def main():
    pwd = get_sf_acc_password()
    print("pwd: `%s`\n" % pwd)

    ctx = snowflake.connector.connect(
        user="vlk",
        password=pwd,
        account="bz10531.eu-north-1.aws",  # account url: https://bz10531.eu-north-1.aws.snowflakecomputing.com
        warehouse="compute_wh",
        database="ecommerce_db",
        schema="ecommerce_dev",
        role="ACCOUNTADMIN",
        session_parameters={"TIMEZONE": "UTC"},
    )

    cs = ctx.cursor()

    try:
        sql = "select * from LINEITEM limit 3"
        cs.execute(sql)
        #   one_row = cs.fetchone()
        #   print(one_row[0])  # first column
        all_rows = cs.fetchall()
        print(all_rows)  # list of tuples
    finally:
        cs.close()

    ctx.close()


def get_sf_acc_password():
    secret_name = "SF_ACC_PASSWORD"
    region_name = "eu-north-1"
    secret = "***"

    def get_secret():
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name,
        )
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
            print("get_secret_value_response: %s\n" % get_secret_value_response)
            printed = """get_secret_value_response:
{'ARN': 'arn:aws:secretsmanager:eu-north-1:443370716175:secret:SF_ACC_PASSWORD-rL9fkV', 
 'Name': 'SF_ACC_PASSWORD', 
 'VersionId': '05936fef-1fda-42cd-af2d-aba9f7de4e28', 
 'SecretString': '{"SF_ACC_PASSWORD":"***"}', 
 'VersionStages': ['AWSCURRENT'], 
 'CreatedDate': datetime.datetime(2024, 10, 15, 16, 53, 57, 860000, tzinfo=tzlocal()), 
 'ResponseMetadata': {
   'RequestId': 'b5f70863-0c73-4a98-aa5a-ca88c4d152ec', 
   'HTTPStatusCode': 200, 
   'HTTPHeaders': {
     'x-amzn-requestid': 'b5f70863-0c73-4a98-aa5a-ca88c4d152ec', 
     'content-type': 'application/x-amz-json-1.1', 
     'content-length': '281', 
     'date': 'Thu, 17 Oct 2024 12:56:45 GMT'}, 
     'RetryAttempts': 0}}
            """
        except ClientError as e:
            # For a list of exceptions thrown, see
            # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            print("error: %s" % e)
            raise e
        secret = get_secret_value_response['SecretString']  # 'SecretString': '{"SF_ACC_PASSWORD":"***"}', 
        print("secret: %s" % secret)  # {"SF_ACC_PASSWORD": "***"}
        return secret
    # Your code goes here.

    secret = get_secret()

    # decode
    secret_obj = json.loads(secret)
    print("secret_obj: %s" % secret_obj)  # secret_obj: {'SF_ACC_PASSWORD': '***'}
    return secret_obj[secret_name]


secrets_manager_notes = """
google: aws glue python secrets passing
got: AWS Glue JOB to get secret value from secretmanager https://stackoverflow.com/questions/74788639/aws-glue-job-to-get-secret-value-from-secretmanager
sm_client = boto3.client('secretsmanager')
response = sm_client.get_secret_value(SecretId=<your_secret_id>)

goto:
https://eu-north-1.console.aws.amazon.com/secretsmanager/landing?region=eu-north-1
create the secret, got:
https://eu-north-1.console.aws.amazon.com/secretsmanager/listsecrets?region=eu-north-1
AWS Secrets Manager / Secrets / SF_ACC_PASSWORD

# Use this code snippet in your app.
# If you need more information about configurations
# or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developer/language/python/
import boto3
from botocore.exceptions import ClientError
def get_secret():
    secret_name = "SF_ACC_PASSWORD"
    region_name = "eu-north-1"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
    secret = get_secret_value_response['SecretString']
    # Your code goes here.

on run got errors:

ClientError: An error occurred (AccessDeniedException) when calling the GetSecretValue operation:
User:
arn:aws:sts::443370716175:assumed-role/aws-glue-service-role/GlueJobRunnerSession
is not authorized to perform:
secretsmanager:GetSecretValue
on resource:
SF_ACC_PASSWORD
because no identity-based policy allows the
secretsmanager:GetSecretValue
action

The policy needs to be created in IAM and attached to the user or role
https://stackoverflow.com/questions/66757368/getsecretvalue-operation-is-not-authorized-error-with-aws-secrets-manager
- Open the IAM Dashboard by searching for IAM on the AWS Search Bar.
- Click on "Users" or "Roles" on the left side.
- Search for the user or role and open it.
- Click "Add Permissions" or "Attach Policies".
...

another error:

DatabaseError: 250001 (08001): Failed to connect to DB: bz10531.eu-north-1.aws.snowflakecomputing.com:443.
Incorrect username or password was specified

and, above all, I can't see my logs ...
> If your AWS Glue jobs don't write logs to CloudWatch, then confirm the following: 
Your AWS Glue job has all the required AWS Identity and Access Management (IAM) permissions. 
The AWS Key Management Service (AWS KMS) key allows CloudWatch Logs to use the key. 
Your job checks the correct CloudWatch log group

role permissions must be adequate, I just add some policies:

IAM / Roles / aws-glue-service-role
Permissions policies (3)
- AmazonAPIGatewayPushToCloudWatchLogs
- AmazonS3FullAccess
- SecretsManagerReadWrite

"""

main()

# next: parameters py script aws glue https://youtu.be/EQ44K5GfgDw?t=3322
