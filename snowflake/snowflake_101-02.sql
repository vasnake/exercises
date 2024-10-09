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
-- stage vs storage integration?

-- create files to upload
use schema ecommerce_db.ecommerce_liv;
select * from LINEITEM limit 10;


-- Ingesting CSV https://youtu.be/EQ44K5GfgDw?t=1297
