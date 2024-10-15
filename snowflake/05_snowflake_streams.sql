-- Introduction to Streams https://youtu.be/EQ44K5GfgDw?t=2198
/*
Stream: a way to implement Change Data Capture
How it is done: DML ops listen, react
Typical data flow: s3 files -> (raw)staging schema -> prod schema
Scenario: ingest new files to staging, catch cdc stream on staging data, apply stream to prod
Use cases for streaming: update search history, stream analytics, anomaly detection, audit transaction log, event-driven platform, ETL for incremental pipelines
*/

-- Implementing Standard Streams https://youtu.be/EQ44K5GfgDw?t=2330
-- create a stream for a table (cdc)
-- view the inserted (captured) data in stream
-- ingest data into production table (from stream)
-- update stage and ingest updated data into prod from stream

-- use role sysadmin;
use role accountadmin;
use warehouse compute_wh;
use database ecommerce_db;
create schema streams_test;
use schema streams_test;

-- staging table
create or replace table members_raw (
  id number(8) not null,
  name varchar(255) default null,
  fee number(3) null
);
-- prod table
create or replace table members_prod (
  id number(8) not null,
  name varchar(255) default null,
  fee number(3) null
);

-- stream
create or replace stream members_std_stream on table members_raw;

select system$stream_get_table_timestamp('members_std_stream') as members_table_st_offset;
-- got '0', empty so far

-- add data to stage
insert into members_raw(id, name, fee) values
  (1, 'Joe', 0),
  (2, 'Jane', 0),
  (3, 'George', 0),
  (4, 'Betty', 0),
  (5, 'Sally', 0);

-- look at the stream
select * from members_std_stream; -- got 5 rows
-- id, name, fee, metadata$action, metadata$isupdate, metadata$row_id
-- 1	Joe	0	INSERT	FALSE	e6671fd81b8b90dbe44b71ae6e89f5fe67aa98d8
-- ...5 Sally ... ca01c2d55ec83d9c1e60c402e7feb8da32360b99

-- add inserted rows, from stream to prod
insert into members_prod(id, name, fee) select id, name, fee
from members_std_stream where metadata$action = 'INSERT'; -- 5 rows inserted
select * from members_prod; -- got 5 rows

select * from members_std_stream; -- got 0 rows, stream is consumed
select system$stream_get_table_timestamp('members_std_stream') as members_table_st_offset;
-- got '1728910186528000001', some sort of offset

-- you can repeat this steps, it works (capture to stream, read-and-delete from stream, add to prod)

-- update staging
update members_raw set fee = 10 where id = 3;
select * from members_std_stream;
/*
already consumed data, in stream it looks like 'delete', 'insert' with 'update=true' flag:
ID	NAME	FEE	METADATA$ACTION	METADATA$ISUPDATE	METADATA$ROW_ID
3	George	10	INSERT	TRUE	54aa23c27595ff5c4632d8c80875589eff5ffc72
3	George	0	DELETE	TRUE	54aa23c27595ff5c4632d8c80875589eff5ffc72

if update (stage) not consumed data, it will be just one 'insert' with 'update=false'
*/

-- merge (upsert) changes to prod from stream
-- if row exists: update it; else: insert it
merge into members_prod as mp
using (select id, name, fee from members_std_stream mstr where metadata$action='INSERT') as mstr
on mp.id = mstr.id
when matched then update set mp.fee = mstr.fee, mp.name=mstr.name
when not matched then insert(id, name, fee) values (mstr.id, mstr.name, mstr.fee::numeric);
/*
number of rows inserted |	number of rows updated
0	1
*/


-- Append-only Streams https://youtu.be/EQ44K5GfgDw?t=2603
-- lets take the last example as a base

use role accountadmin;
use warehouse compute_wh;
use database ecommerce_db;
create or replace schema streams_test;
use schema streams_test;

-- staging table
create or replace table members_raw (
  id number(8) not null,
  name varchar(255) default null,
  fee number(3) null
);

-- prod table
create or replace table members_prod (
  id number(8) not null,
  name varchar(255) default null,
  fee number(3) null
);

-- stream (append_only)
create or replace stream members_append_stream on table members_raw append_only=true;

-- add data to stage
insert into members_raw(id, name, fee) values
  (1, 'Joe', 0),
  (2, 'Jane', 0),
  (3, 'George', 0),
  (4, 'Betty', 0),
  (5, 'Sally', 0)
;

select * from members_append_stream;
/*
ID	NAME	FEE	METADATA$ACTION	METADATA$ISUPDATE	METADATA$ROW_ID
1	Joe	0	INSERT	FALSE	c35e8e698ebf244f5ee12b64cd6d86ed14696c23
2	Jane	0	INSERT	FALSE	44d6105b6570088f15b8ca6ad7452408201e9aba
3	George	0	INSERT	FALSE	42f66726e127221d42f132fd18014bd41c627054
4	Betty	0	INSERT	FALSE	2c94557f06d4fee921903fc794a86cc3764631fe
5	Sally	0	INSERT	FALSE	0b2dc7567d136d37f9d66fa8724049dc9e0ccf78
*/

-- offset is zero: not consumed any rows from stream yet
select system$stream_get_table_timestamp('members_append_stream') as members_table_stream_offset;
/*
MEMBERS_TABLE_STREAM_OFFSET
0
*/

-- add inserted rows, from stream to prod, easy, w/o condition, only added rows in stream
insert into members_prod(id, name, fee) select id, name, fee from members_append_stream;
-- stream now is empty
-- offset?
select to_timestamp(system$stream_get_table_timestamp('members_append_stream')) as members_table_stream_offset;
/*
MEMBERS_TABLE_STREAM_OFFSET
2024-10-14 14:55:52.916
*/

-- lets try update and delete
update members_raw set fee = 10 where id = 3;
select * from members_append_stream; -- no results (append_only stream, remember?)

-- Connect Python with Snowflake on localhost https://youtu.be/EQ44K5GfgDw?t=2798
-- 06_snowflake-connector-python.py
