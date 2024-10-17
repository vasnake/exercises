"""
parameters for py script on aws glue https://youtu.be/EQ44K5GfgDw?t=3322

prev: 07_snowflake-etl-aws-glue-secret.py
next: pandas + snowflake + aws_glue https://youtu.be/EQ44K5GfgDw?t=3505

show how to pass 2 parameters to sql query

https://eu-north-1.console.aws.amazon.com/gluestudio/home?region=eu-north-1#/editor/job/sf-connector/details
job-details / advanced properties / job parameters
add key-value pairs,
keys (in py code omit dashes):
--supplier_key, --ship_date
values: optional, leave it empty.

in py code:

args = getResolvedOptions(sys.argv, ["supplier_key", "ship_date"])  # https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-get-resolved-options.html
ship_date = args["ship_date"]
supplier_key = args["supplier_key"]
sql_query = "select * from lineitem where l_shipdate='{0}' and l_suppkey='{1}'".format(ship_date, supplier_key)

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
    args = getResolvedOptions(sys.argv, ["supplier_key", "ship_date"])  # https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-get-resolved-options.html
    # fail if params not passed

    ship_date = args["ship_date"]  # 1992-01-21
    supplier_key = args["supplier_key"]  # 3693268

    pwd = get_sf_acc_password()
    print("pwd: `%s`\n" % pwd)

    sql_query = """select * from LINEITEM
        where l_shipdate='{0}' and l_suppkey='{1}'
        limit 3""".format(ship_date, supplier_key)
    print("query: %s\n" % sql_query)

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
        cs.execute(sql_query)
        #   one_row = cs.fetchone()
        #   print(one_row[0])  # first column

        # why we added next two lines? no explanation
        query_id = cs.sfqid
        cs.get_results_from_sfqid(query_id)

        all_rows = cs.fetchall()
        print(all_rows)  # list of tuples
    except Exception as e:
        ctx.rollback()
        print("error: %s" % e)
        raise e
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
        except ClientError as e:
            # For a list of exceptions thrown, see
            # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            print("error: %s" % e)
            raise e
        secret = get_secret_value_response['SecretString']
        print("secret: %s" % secret)
        return secret
    # Your code goes here.

    secret = get_secret()

    # decode
    secret_obj = json.loads(secret)
    print("secret_obj: %s" % secret_obj)
    return secret_obj[secret_name]


lecture_notes = """
on first run got an error:

usage: sf-connector.py [-h] --supplier_key SUPPLIER_KEY --ship_date SHIP_DATE
sf-connector.py: error: the following arguments are required: --supplier_key, --ship_date

in sql worksheet, run:

use role accountadmin;
use database ecommerce_db;
use schema ecommerce_db.ecommerce_dev;
select distinct l_shipdate, l_suppkey from lineitem limit 3;

got
L_SHIPDATE	L_SUPPKEY
1992-01-08	4399131
1992-01-21	3693268
1992-01-24	5097773

add values to keys in 'job-details / advanced properties / job parameters'
"""

main()
