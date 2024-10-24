# zero copy clone https://youtu.be/EQ44K5GfgDw?t=4318

`create or replace table qa_customer clone prod_customer;`
quick and easy

docs

https://docs.snowflake.com/en/sql-reference/sql/create-clone
> for creating zero-copy clones of databases, schemas, and tables.

https://docs.snowflake.com/en/user-guide/tables-storage-considerations#label-cloning-tables
> Snowflake’s zero-copy cloning feature provides a convenient way to quickly take a “snapshot” of any table, schema, or database and create a derived copy of that object which initially shares the underlying storage. This can be extremely useful for creating instant backups that do not incur any additional costs (until changes are made to the cloned object).

Clone = zero copy clone: just a snapshot.

# clone vs snapshot https://youtu.be/EQ44K5GfgDw?t=4453

They are the same, clone is done using snapshot technique.
Changes made to V1 or V2 after cloning: don't affect other copy.

# schema level cloneable objects https://youtu.be/EQ44K5GfgDw?t=4561

`create database target_db clone source_db;`
db roles and grants NOT cloned.
What cloned: schema's:
- roles and grants
- tables
- file formats
- sequences
- named external stages
- pipes
- streams
- tasks

Cloneable:
database, table, schama, stream;
stage, file_format, sequence, tast

transient? temp? Not without some tricks.

# simple table cloning https://youtu.be/EQ44K5GfgDw?t=4676

Create demo (base) table and fill it
```sql
create database ecommerce;
use database ecommerce;
create schema e_dev;
use schema e_dev;

create or replace table simple_order (
  orderkey number(38,0),
  custkey number(38,0),
  orderstatus varchar(1),
  totalprice number(12,2),
  orderdate date,
  orderpriority varchar(15)
);

insert into simple_order(orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority)
  select o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority
  from snowflake_sample_data.tpch_sf1.orders
  order by o_orderkey limit 10000;
```

Some checks
```sql
select * from simple_order limit 9;
/*
ORDERKEY	CUSTKEY	ORDERSTATUS	TOTALPRICE	ORDERDATE	ORDERPRIORITY
1	36901	O	173665.47	1996-01-02	5-LOW
2	78002	O	46929.18	1996-12-01	1-URGENT
3	123314	F	193846.25	1993-10-14	5-LOW
4	136777	O	32151.78	1995-10-11	5-LOW
5	44485	F	144659.20	1994-07-30	5-LOW
6	55624	F	58749.59	1992-02-21	4-NOT SPECIFIED
7	39136	O	252004.18	1996-01-10	2-HIGH
32	130057	O	208660.75	1995-07-16	2-HIGH
33	66958	F	163243.98	1993-10-27	3-MEDIUM
*/
```

Make a changes, for time-travel metrics
`delete from simple_order where orderkey between 30 and 40;`
```
number of rows deleted
8
```

Create cloned table and run some tests
```sql
create table simple_order_clone clone simple_order;
-- Table SIMPLE_ORDER_CLONE successfully created.

select count(1) from simple_order; -- 9992
select count(1) from simple_order_clone; -- 9992

select * from ECOMMERCE.INFORMATION_SCHEMA.TABLES
  where table_name like 'SIMPLE_ORDER%' and table_schema = 'E_DEV';
/*
TABLE_CATALOG	TABLE_SCHEMA	TABLE_NAME	TABLE_OWNER	TABLE_TYPE	IS_TRANSIENT	CLUSTERING_KEY	ROW_COUNT	BYTES	RETENTION_TIME	SELF_REFERENCING_COLUMN_NAME	REFERENCE_GENERATION	USER_DEFINED_TYPE_CATALOG	USER_DEFINED_TYPE_SCHEMA	USER_DEFINED_TYPE_NAME	IS_INSERTABLE_INTO	IS_TYPED	COMMIT_ACTION	CREATED	LAST_ALTERED	LAST_DDL	LAST_DDL_BY	AUTO_CLUSTERING_ON	COMMENT	IS_TEMPORARY	IS_ICEBERG	IS_DYNAMIC	IS_IMMUTABLE
ECOMMERCE	E_DEV	SIMPLE_ORDER	ACCOUNTADMIN	BASE TABLE	NO		9992	120320	1						YES	YES		2024-10-24 06:25:24.850 -0700	2024-10-24 06:28:20.277 -0700	2024-10-24 06:25:24.850 -0700	VLK	NO		NO	NO	NO	NO
ECOMMERCE	E_DEV	SIMPLE_ORDER_CLONE	ACCOUNTADMIN	BASE TABLE	NO		9992	120320	1						YES	YES		2024-10-24 06:30:04.023 -0700	2024-10-24 06:30:04.585 -0700	2024-10-24 06:30:04.023 -0700	VLK	NO		NO	NO	NO	NO
*/

select * from SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
  where table_schema = 'E_DEV' and table_name like 'SIMPLE_ORDER%' limit 9;
/*
ID	TABLE_NAME	TABLE_SCHEMA_ID	TABLE_SCHEMA	TABLE_CATALOG_ID	TABLE_CATALOG	CLONE_GROUP_ID	IS_TRANSIENT	ACTIVE_BYTES	TIME_TRAVEL_BYTES	FAILSAFE_BYTES	RETAINED_FOR_CLONE_BYTES	DELETED	TABLE_CREATED	TABLE_DROPPED	TABLE_ENTERED_FAILSAFE	SCHEMA_CREATED	SCHEMA_DROPPED	CATALOG_CREATED	CATALOG_DROPPED	COMMENT	INSTANCE_ID
19458	SIMPLE_ORDER	54	E_DEV	16	ECOMMERCE	19458	NO	0	0	0	0	FALSE	2024-10-24 06:25:24.850 -0700			2024-10-24 06:25:18.199 -0700		2024-10-24 06:25:17.462 -0700			
20482	SIMPLE_ORDER_CLONE	54	E_DEV	16	ECOMMERCE	19458	NO	0	0	0	0	FALSE	2024-10-24 06:30:04.023 -0700			2024-10-24 06:25:18.199 -0700		2024-10-24 06:25:17.462 -0700			
*/
```

Lets made some changes in tables and see what changes in metrics
```sql
insert into simple_order(orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority)
  select o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority
  from snowflake_sample_data.tpch_sf1.orders
  order by o_orderkey limit 1000;

update simple_order_clone
  set orderpriority = '1-URGENT'
  where orderkey > 10000 and orderpriority <> '1-URGENT';
/*
number of rows updated	number of multi-joined rows updated
6007	0
*/

select * from ECOMMERCE.INFORMATION_SCHEMA.TABLES
  where table_name like 'SIMPLE_ORDER%' and table_schema = 'E_DEV';
/*
ROW_COUNT	BYTES
10992	132096
9992	109056
*/

select * from SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
  where table_schema = 'E_DEV' and table_name like 'SIMPLE_ORDER%';
/*
ACTIVE_BYTES	TIME_TRAVEL_BYTES
0	0
0	0
weird, should be != 0
*/
```

# transient and temporary tables cloning https://youtu.be/EQ44K5GfgDw?t=5009

possible clones:
- permanent -> transient or temporary,
- transient -> transient or temporary,
- temp -> temp or trans

impossible: trans or temp -> permanent

clone permanent
```sql
use database ecommerce;
use schema e_dev;

create or replace temporary table so_temp_from_base clone simple_order;
create or replace transient table so_trans_from_base clone simple_order;
```

clone transient
```sql
create or replace transient table simple_order_trans (
  orderkey number(38,0),
  custkey number(38,0),
  orderstatus varchar(1),
  totalprice number(12,2),
  orderdate date,
  orderpriority varchar(15)
);
insert into simple_order_trans(orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority)
  select o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority
  from snowflake_sample_data.tpch_sf1.orders
  order by o_orderkey limit 100;
select * from simple_order_trans limit 9;

-- SQL compilation error: Transient object cannot be cloned to a permanent object
create table so_base_from_trans clone simple_order_trans; -- failed, can't do such thing

create transient table so_trans_from_trans clone simple_order_trans;
create temporary table so_temp_from_trans clone simple_order_trans;

show tables like '%_trans';
/*
created_on	name	database_name	schema_name	kind	comment	cluster_by	rows	bytes	owner	retention_time	automatic_clustering	change_tracking	is_external	enable_schema_evolution	owner_role_type	is_event	budget	is_hybrid	is_iceberg	is_dynamic	is_immutable
2024-10-24 07:47:31.047 -0700	SIMPLE_ORDER_TRANS	ECOMMERCE	E_DEV	TRANSIENT			100	3584	ACCOUNTADMIN	1	OFF	OFF	N	N	ROLE	N		N	N	N	N
2024-10-24 07:48:47.954 -0700	SO_TEMP_FROM_TRANS	ECOMMERCE	E_DEV	TEMPORARY			100	3584	ACCOUNTADMIN	1	OFF	OFF	N	N	ROLE	N		N	N	N	N
2024-10-24 07:48:43.017 -0700	SO_TRANS_FROM_TRANS	ECOMMERCE	E_DEV	TRANSIENT			100	3584	ACCOUNTADMIN	1	OFF	OFF	N	N	ROLE	N		N	N	N	N
*/
```

clone temporary
```sql
create or replace temporary table simple_order_temp (
  orderkey number(38,0),
  custkey number(38,0),
  orderstatus varchar(1),
  totalprice number(12,2),
  orderdate date,
  orderpriority varchar(15)
);
insert into simple_order_temp(orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority)
  select o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority
  from snowflake_sample_data.tpch_sf1.orders
  order by o_orderkey limit 100;
select * from simple_order_temp limit 9;

create or replace temporary table so_temp_from_temp clone simple_order_temp;
create or replace transient table so_trans_from_temp clone simple_order_temp;

-- SQL compilation error: Temp table cannot be cloned to a permanent table; clone to a transient table instead
create table so_base_from_temp clone simple_order_temp; -- failed

show tables like '%_temp';
/*
created_on	name	database_name	schema_name	kind	comment	cluster_by	rows	bytes	owner	retention_time	automatic_clustering	change_tracking	is_external	enable_schema_evolution	owner_role_type	is_event	budget	is_hybrid	is_iceberg	is_dynamic	is_immutable
2024-10-24 07:50:05.496 -0700	SIMPLE_ORDER_TEMP	ECOMMERCE	E_DEV	TEMPORARY			100	3584	ACCOUNTADMIN	1	OFF	OFF	N	N	ROLE	N		N	N	N	N
2024-10-24 07:50:40.054 -0700	SO_TEMP_FROM_TEMP	ECOMMERCE	E_DEV	TEMPORARY			100	3584	ACCOUNTADMIN	1	OFF	OFF	N	N	ROLE	N		N	N	N	N
2024-10-24 07:50:41.268 -0700	SO_TRANS_FROM_TEMP	ECOMMERCE	E_DEV	TRANSIENT			100	3584	ACCOUNTADMIN	1	OFF	OFF	N	N	ROLE	N		N	N	N	N
*/
```

-- next: object dependency with clone https://youtu.be/EQ44K5GfgDw?t=5264
