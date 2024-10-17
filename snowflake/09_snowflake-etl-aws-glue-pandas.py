"""
pandas + snowflake + aws_glue https://youtu.be/EQ44K5GfgDw?t=3505

prev: 08_snowflake-etl-aws-glue-parameters.py
next: setup kafka on localhost https://youtu.be/EQ44K5GfgDw?t=3678

all the changes in py script:

import pandas as pd
sql = "..."
pdf = pd.read_sql(sql, ctx)
print(pdf.head())
"""

# https://eu-north-1.console.aws.amazon.com/gluestudio/home?region=eu-north-1#/editor/job/sf-connector/script

import sys
import os
import json
import snowflake.connector
import boto3
import pandas as pd

from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions


def main():
    sql_query = """select * from LINEITEM where l_shipdate='{0}' and l_suppkey='{1}' limit 3"""  # really? placeholders?

    args = getResolvedOptions(sys.argv, ["supplier_key", "ship_date"])
    ship_date = args["ship_date"]  # 1992-01-21
    supplier_key = args["supplier_key"]  # 3693268

    sql_query = sql_query.format(ship_date, supplier_key)
    print("query: %s\n" % sql_query)

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

    try:
        pdf = pd.read_sql(sql_query, ctx)
        print(pdf.head())
    except Exception as e:
        ctx.rollback()
        print("error: %s" % e)
        raise e
    finally:
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
"""

main()
