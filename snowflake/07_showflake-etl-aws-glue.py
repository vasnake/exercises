"""
py script on aws glue https://youtu.be/EQ44K5GfgDw?t=3044

Setup and run py shell script (see prev. step) on aws glue
We need WHL file to add snowflake.connector lib to env, that file should be loaded to s3 bucket to use in script setup.
How to create whl (snowflake_connector_python-2.3.8-py3-none-any.whl 330 KB): see later.

Script to run: previos script with one line added:
from awsglue.utils import getResolvedOptions

goto aws glue (https://eu-north-1.console.aws.amazon.com/glue/home?region=eu-north-1#/v2/getting-started)
aws glue studio
python shell script editor

upload or create script
'sf-connector'

IAM role: aws-glue-service-role
advanced options:
script filename sf-connector
python library path: s3 path for whl file loaded to bucket (later, buckets/sl-glue-job1/whl/snowflake_connector_python-2.3.8-py3-none-any.whl) 'copy s3 uri'

'run' ...

-- create whl and upload to (https://eu-north-1.console.aws.amazon.com/s3/buckets/vlk-snowflake-bucket?region=eu-north-1&bucketType=general&prefix=whl/&showversions=false)
snowflake_connector_python-2.3.8-py3-none-any.whl ???
fuck it, can do much more easy:
Installing additional Python modules with pip in AWS Glue 2.0+ https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html?icmpid=docs_console_unmapped#addl-python-modules-support
added job parameter: `--additional-python-modules snowflake-connector-python`

'run' ... password for connection?

job run input arguments:
--enable-job-insights	false
--additional-python-modules	snowflake-connector-python
--enable-observability-metrics	false
--enable-glue-datacatalog	true
library-set	analytics
--job-language	python
--TempDir	s3://aws-glue-assets-443370716175-eu-north-1/temporary/
SF_ACC_PASSWORD	foo
"""

# https://eu-north-1.console.aws.amazon.com/gluestudio/home?region=eu-north-1#/editor/job/sf-connector/details

import sys
import os
import snowflake.connector
from awsglue.utils import getResolvedOptions

pwd = os.getenv("SF_ACC_PASSWORD", "***")
if pwd == "***":
  raise EnvironmentError("Environment variable 'SF_ACC_PASSWORD' is not set")
help = """
aws glue python secrets passing
AWS Glue JOB to get secret value from secretmanager https://stackoverflow.com/questions/74788639/aws-glue-job-to-get-secret-value-from-secretmanager
sm_client = boto3.client('secretsmanager')
response = sm_client.get_secret_value(
    SecretId=<your_secret_id>
)

https://eu-north-1.console.aws.amazon.com/secretsmanager/landing?region=eu-north-1
https://eu-north-1.console.aws.amazon.com/secretsmanager/listsecrets?region=eu-north-1

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

"""

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

# parameters py script aws glue https://youtu.be/EQ44K5GfgDw?t=3322
