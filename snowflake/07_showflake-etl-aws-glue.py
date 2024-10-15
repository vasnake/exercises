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
