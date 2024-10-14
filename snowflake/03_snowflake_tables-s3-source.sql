-- https://www.youtube.com/watch?v=EQ44K5GfgDw

-- tables
-- permanent (by default), transient
use role accountadmin;
create database if not exists ecommerce_db;
create schema if not exists ecommerce_liv;
use schema ecommerce_db.ecommerce_liv;
use warehouse COMPUTE_WH;
-- transient table: avoid time-travel, backup, and recovery costs
create or replace transient table transient_orders as select * from TASTY_BYTES_SAMPLE_DATA.RAW_POS.MENU limit 13;

-- views
-- simple, materialized, secured
create or replace view simple_view as select * from transient_orders;
create or replace materialized view mat_view as select * from transient_orders;
alter ... cluster by (order_date);

-- micropartitions, clustering keys
-- https://youtu.be/EQ44K5GfgDw?feature=shared&t=608

use schema SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000; -- big tables
-- system$: builtin function
select system$clustering_information('LINEITEM');
show tables like '%LINE%';

alter session set USE_CACHED_RESULT=False; -- don't spoil query profile details
-- see query details, query profile: bytes scanned, partitions scanned ...
select * from LINEITEM limit 10000; -- 1 part. scanned
select * from LINEITEM where l_shipdate in ('1998-12-01', '1998-09-20') limit 10000; -- should be 1 part scanned, but no

select system$clustering_information('PARTSUPP','PS_SUPPKEY'); -- should be clustered, but no
select count(1), count(distinct ps_suppkey) from partsupp; -- 800 000 000, 10 000 000
-- 80 keys in one bin on average, good for clustering? I don't think so, high cardinality

-- copy to sandbox and add clustering
use schema ecommerce_db.ecommerce_liv;
create or replace table LINEITEM cluster by (l_shipdate) as select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.LINEITEM limit 100000;
-- nation, part, partsupp, region, supplier, customer
select system$clustering_information('LINEITEM');
-- {
--   "cluster_by_keys" : "LINEAR(l_shipdate)",
--   "notes" : "Clustering key columns contain high cardinality key L_SHIPDATE which might result in expensive re-clustering. Consider reducing the cardinality of clustering keys. Please refer to https://docs.snowflake.net/manuals/user-guide/tables-clustering-keys.html for more information.",
--   "total_partition_count" : 8,
--   "total_constant_partition_count" : 0,
--   "average_overlaps" : 0.0,
--   "average_depth" : 1.0,
--   "partition_depth_histogram" : {
--     "00000" : 0,
-- ...
--     "00016" : 0
--   },
--   "clustering_errors" : [ ]
-- }


-- AWS S3 -> Snowflake, integration object https://youtu.be/EQ44K5GfgDw?feature=shared&t=921
-- create IAM role (AWS)
-- create S3 bucket and add sample data
-- create integration object (Snowflake)

-- https://eu-north-1.console.aws.amazon.com/console/home?region=eu-north-1#
-- in 'Services' input, type 'IAM', click 'IAM manage access ...', IAM Dashboard opens;
-- roles, create role
-- AWS account, require external id (00000), next
-- add permission: type 's3', select 'QuickSightAccessForS3StorageManagementAnalyticsReadOnly', next
-- role name: snowflake-aws-role, create ...
-- https://us-east-1.console.aws.amazon.com/iam/home?region=eu-north-1#/roles
-- ARN: arn:aws:iam::443370716175:role/snowflake-aws-role

-- in service, type 's3', select 'S3 storage ...'
-- create bucket 'snowflake-bucket', uncheck 'block all public access', create
-- check that bucket region the same as for snowflake
-- click on bucket, 'create folder' 'ecommerce_dev/lineitem', inside 'csv', 'json', 'parquet' folders
-- s3://vlk-snowflake-bucket/ecommerce_dev/lineitem/csv/
-- bucket ARN: arn:aws:s3:::vlk-snowflake-bucket

-- create files to upload ... see next section, 'copy ...'
-- upload files ... skip for the moment ...

-- to create integration object, you need accountadmin
use role accountadmin;
create storage integration aws_sf_data
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::443370716175:role/snowflake-aws-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://vlk-snowflake-bucket');

-- find info to add to aws
desc integration aws_sf_data;
-- STORAGE_AWS_IAM_USER_ARN: arn:aws:iam::905418160770:user/e1fn0000-s
-- STORAGE_AWS_EXTERNAL_ID: BZ10531_SFCRole=3_mLHN6alq+SISNuBypMs9YPojHR0=

-- goto aws
-- https://us-east-1.console.aws.amazon.com/iam/home?region=eu-north-1#/roles/details/snowflake-aws-role?section=permissions
-- trust relationships, edit trust policy

-- "Statement" / "Principal" / "AWS": "arn:aws:iam::443370716175:root" =>
-- "Statement" / "Principal" / "AWS": "arn:aws:iam::905418160770:user/e1fn0000-s"

-- "Statement" / "Condition" / "StringEquals" / "sts:ExternalId": "00000" =>
-- "Statement" / "Condition" / "StringEquals" / "sts:ExternalId": "BZ10531_SFCRole=3_mLHN6alq+SISNuBypMs9YPojHR0="

-- earlier I created stage
-- CREATE OR REPLACE STAGE tasty_bytes_sample_data.public.blob_stage url = 's3://sfquickstarts/tastybytes/' file_format = (type = csv);
-- stage vs storage integration? see next section ...


-- Ingesting CSV from S3 to Snowflake https://youtu.be/EQ44K5GfgDw?t=1297
-- create new empty table, copy of prod table
-- create file format
-- create stage
-- copy into X from Y

-- use role sysadmin;
use role accountadmin;
use database ecommerce_db;
create schema ecommerce_dev;
use schema ecommerce_db.ecommerce_dev;
create table lineitem cluster by (L_SHIPDATE) as select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM" limit 1;
truncate table lineitem;

create file format csv_load_format
  type = 'CSV'
  compression = 'AUTO'
  field_delimiter = ','
  record_delimiter = '\n'
  skip_header = 1
  field_optionally_enclosed_by = '\042'
  trim_space = false
  error_on_column_count_mismatch = true
  escape = 'NONE'
  escape_unenclosed_field = '\134'
  date_format = 'AUTO'
  timestamp_format = 'AUTO';
desc file format csv_load_format;

create stage stage_csv_dev
  storage_integration = aws_sf_data -- one line difference with earlier 'create stage ...'
  url = 's3://vlk-snowflake-bucket/ecommerce_dev/lineitem/csv/'
  file_format = csv_load_format;
-- url from bucket page: https://eu-north-1.console.aws.amazon.com/s3/buckets/vlk-snowflake-bucket?prefix=ecommerce_dev/lineitem/&region=eu-north-1&bucketType=general
desc stage stage_csv_dev;

list @stage_csv_dev;
-- access denied, goto
-- https://us-east-1.console.aws.amazon.com/iam/home?region=eu-north-1#/roles/details/snowflake-aws-role?section=permissions
-- add AmazonS3FullAccess policy to permissions policies

copy into @stage_csv_dev from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM";
list @stage_csv_dev;
-- name: s3://vlk-snowflake-bucket/ecommerce_dev/lineitem/csv/data_0_0_0.csv.gz
-- arn:aws:s3:::vlk-snowflake-bucket/ecommerce_dev/lineitem/csv/data_0_0_0.csv.gz

use schema ecommerce_db.ecommerce_dev;
copy into lineitem from @stage_csv_dev on_error = abort_statement;
-- rows_loaded: 99999
select * from lineitem limit 10;
select count(1) from lineitem;

-- next: https://youtu.be/EQ44K5GfgDw?t=1656

-- Ingest JSON, S3 -> SF: https://youtu.be/EQ44K5GfgDw?t=1656
-- create file format
-- create stage
-- (create files on S3, optional)
-- create temp table with one variant column
-- copy into temp table
-- insert parsed (from temp table) json to target

-- use role sysadmin;
use role accountadmin;
use schema ecommerce_db.ecommerce_dev;

create or replace file format json_load_format
  type = 'JSON';

-- goto https://eu-north-1.console.aws.amazon.com/s3/buckets/vlk-snowflake-bucket?prefix=ecommerce_dev/lineitem/&region=eu-north-1&bucketType=general
-- create folder lineitem/json

create stage stage_json_dev
  storage_integration = aws_sf_data
  url = 's3://vlk-snowflake-bucket/ecommerce_dev/lineitem/json/'
  file_format = json_load_format;
desc stage stage_json_dev;
list @stage_json_dev;

-- create files in S3

copy into @stage_json_dev from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM";
-- Unsupported feature 'unloading of more than one column or non-json values'.
-- https://stackoverflow.com/questions/76590915/using-snowflakes-copy-into-command-populate-data-to-specific-columns
-- problem: it needs valid json, get one:
select To_JSON(object_construct(*)) as json_obj from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM" limit 10;

-- 'copy into' not working with cte
with cte (json_obj) as (
  select To_JSON(object_construct(*)) from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM"
) select * from cte limit 10; -- copy into @stage_json_dev from cte;

-- works OK
copy into @stage_json_dev from (
  select object_construct(*) as json_obj from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM"
);
-- https://docs.snowflake.com/en/sql-reference/sql/copy-into-location

-- files ready

use schema ecommerce_db.ecommerce_dev;
copy into lineitem from @stage_json_dev on_error = abort_statement;
-- compile error: JSON file format can produce one and only one column of type variant, object, or array. Load data into separate columns using the MATCH_BY_COLUMN_NAME copy option or copy with transformation.

-- lets explore, select first column
select $1 from @stage_json_dev limit 10;
/* one row text: {
  "L_COMMENT": "e bravely even packages. furiously e",
  "L_COMMITDATE": "1992-04-01",
  "L_DISCOUNT": 0.03,
  "L_EXTENDEDPRICE": 19915.2,
  "L_LINENUMBER": 3,
  "L_LINESTATUS": "F",
  "L_ORDERKEY": 2164615200,
  "L_PARTKEY": 174815421,
  "L_QUANTITY": 15,
  "L_RECEIPTDATE": "1992-01-07",
  "L_RETURNFLAG": "A",
  "L_SHIPDATE": "1992-01-03",
  "L_SHIPINSTRUCT": "TAKE BACK RETURN",
  "L_SHIPMODE": "FOB",
  "L_SUPPKEY": 7315439,
  "L_TAX": 0.01
} */
-- json object, row from src table
-- suggestion: ELT, load into temp table as is, transform-load to target table

-- create temp table
create table lineitem_raw_json (src variant);
copy into lineitem_raw_json from @stage_json_dev on_error = abort_statement;
-- file: s3://vlk-snowflake-bucket/ecommerce_dev/lineitem/json/data_0_0_0.json.gz
-- rows_loaded: 100000

-- check
select * from lineitem_raw_json limit 3;
/*{
  "L_COMMENT": "e bravely even packages. furiously e",
  "L_COMMITDATE": "1992-04-01",
  "L_DISCOUNT": 0.03,
  "L_EXTENDEDPRICE": 19915.2,
  "L_LINENUMBER": 3,
  "L_LINESTATUS": "F",
  "L_ORDERKEY": 2164615200,
  "L_PARTKEY": 174815421,
  "L_QUANTITY": 15,
  "L_RECEIPTDATE": "1992-01-07",
  "L_RETURNFLAG": "A",
  "L_SHIPDATE": "1992-01-03",
  "L_SHIPINSTRUCT": "TAKE BACK RETURN",
  "L_SHIPMODE": "FOB",
  "L_SUPPKEY": 7315439,
  "L_TAX": 0.01
}*/

-- get table columns names and they order
desc table lineitem;
SELECT GET_DDL('table', 'ECOMMERCE_DB.ECOMMERCE_DEV.LINEITEM');
/*
create or replace TABLE LINEITEM cluster by (L_SHIPDATE)(
	L_ORDERKEY NUMBER(38,0),
	L_PARTKEY NUMBER(38,0),
	L_SUPPKEY NUMBER(38,0),
	L_LINENUMBER NUMBER(38,0),
	L_QUANTITY NUMBER(12,2),
	L_EXTENDEDPRICE NUMBER(12,2),
	L_DISCOUNT NUMBER(12,2),
	L_TAX NUMBER(12,2),
	L_RETURNFLAG VARCHAR(1),
	L_LINESTATUS VARCHAR(1),
	L_SHIPDATE DATE,
	L_COMMITDATE DATE,
	L_RECEIPTDATE DATE,
	L_SHIPINSTRUCT VARCHAR(25),
	L_SHIPMODE VARCHAR(10),
	L_COMMENT VARCHAR(44)
);
*/

-- produce this stupid query
insert into lineitem
select -- order of columns is crucial
	SRC:L_ORDERKEY,
	SRC:L_PARTKEY,
	SRC:L_SUPPKEY,
	SRC:L_LINENUMBER,
	SRC:L_QUANTITY,
	SRC:L_EXTENDEDPRICE,
	SRC:L_DISCOUNT,
	SRC:L_TAX,
	SRC:L_RETURNFLAG,
	SRC:L_LINESTATUS,
	SRC:L_SHIPDATE,
	SRC:L_COMMITDATE,
	SRC:L_RECEIPTDATE,
	SRC:L_SHIPINSTRUCT,
	SRC:L_SHIPMODE,
	SRC:L_COMMENT
from lineitem_raw_json;

select * from lineitem limit 3;
