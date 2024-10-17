"""
Connect Python with Snowflake on localhost https://youtu.be/EQ44K5GfgDw?t=2798
06_snowflake-connector-python.py

python connector api https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api
Module: snowflake.connector

pip install snowflake-connector-python
python3 06_snowflake-connector-python.py 
"""

import os
import snowflake.connector

pwd = os.getenv("SF_ACC_PASSWORD", "***")
if pwd == "***":
  raise EnvironmentError("Environment variable 'SF_ACC_PASSWORD' is not set")
print("secret: `%s`\n" % pwd)

ctx = snowflake.connector.connect(
  user="vlk",
  password=pwd,
#   account="BPGRKTT-EY00038",  # account identifier: organization.account # not working
  account="bz10531.eu-north-1.aws",  # account url: https://bz10531.eu-north-1.aws.snowflakecomputing.com
  warehouse="compute_wh",
  database="ecommerce_db",
  schema="ecommerce_dev",
  role="ACCOUNTADMIN",
  session_parameters={"TIMEZONE": "UTC"}
)

cs = ctx.cursor()
try:
  sql = "select * from LINEITEM limit 10"
  cs.execute(sql)

  one_row = cs.fetchone()
  print(one_row[0])  # first column

  all_rows = cs.fetchall()
  print(all_rows)  # list of tuples

finally:
  cs.close()

ctx.close()

# py script on aws glue https://youtu.be/EQ44K5GfgDw?t=3044
# 07_showflake-etl-aws-glue.py
