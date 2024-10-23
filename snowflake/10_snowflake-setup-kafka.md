# setup kafka on localhost https://youtu.be/EQ44K5GfgDw?t=3678

steps
- zookeeper running
- kafka running
- create a topic
- produce, consume a topic messages

```bat
kafka 2.12 3.4.0> bin/windows/zookeeper-server-start.bat ./config/zookeeper.properties
kafka 2.12 3.4.0> bin/windows/kafka-server-start.bat ./config/server.properties
kafka 2.12 3.4.0> bin/windows/kafka-topic.bat --create --topic test-data --bootstrap-server localhost 9092
kafka 2.12 3.4.0> bin/windows/kafka-console-producer.bat --topic test-data --bootstrap-server localhost 9092
kafka 2.12 3.4.0> bin/windows/kafka-console-consumer.bat --topic test-data --from-beginning --bootstrap-server localhost 9092
```

I have to do this steps (wsl):
```s
- goto https://kafka.apache.org/quickstart
- download kafka_2.13-3.8.0.tgz

pushd /mnt/c/Users/valik/Downloads/
tar -xzf kafka_2.13-3.8.0.tgz
pushd kafka_2.13-3.8.0/

bin/zookeeper-server-start.sh config/zookeeper.properties &
bin/kafka-server-start.sh config/server.properties &

# [2024-10-18 19:03:08,593] INFO Awaiting socket connections on 0.0.0.0:9092. (kafka.network.DataPlaneAcceptor)
netstat -tanl

bin/kafka-topics.sh --create --topic test-data --bootstrap-server :::9092
bin/kafka-console-producer.sh --topic test-data --bootstrap-server :::9092
bin/kafka-console-consumer.sh --topic test-data --from-beginning --bootstrap-server :::9092
```

# kafka - snowflake connector https://youtu.be/EQ44K5GfgDw?t=3829

steps to config kafka - snowflake connector
- create SF_connector.properties file
- config connect-standalone.properties file

`vim ./config/SF_connector.properties`
```properties
connector.class=com.snowflake.kafka.connector.SnowflakeSinkConnector
key.converter=com.snowflake.kafka.connector.records.SnowflakeJsonConverter
value.converter=com.snowflake.kafka.connector.records.SnowflakeJsonConverter
tasks.max=8
topics=sales-data
snowflake.topic2table.map=sales-data:sales_data
buffer.count.records=10000
buffer.flash.time=60
buffer.size.bytes=5000000
snowflake.url.name=https://bz10531.eu-north-1.aws.snowflakecomputing.com:443 # from account url
snowflake.user.name=vlk
snowflake.private.key=
snowflake.database.name=ecommerce_db
snowflake.schema.name=kafka_live_streaming
name=kafka_live_streaming
```

`vim ./config/connect-standalone.properties`
```properties
plugin.path=/mnt/c/Users/valik/Downloads/kafka_2.13-3.8.0/libs
bootstrap.servers=:::9092
```

# setup encription keys https://youtu.be/EQ44K5GfgDw?t=3986

setup encription, kafka+snowflake, steps
- create RSA private key
- create RSA public key
- assign public key to user in SF

Where I get keys? Use `ssh-keygen`, or:
Online RSA Key Generator http://travistidwell.com/jsencrypt/demo/

Where I should put private key:
```s
pushd /mnt/c/Users/valik/Downloads/kafka_2.13-3.8.0/
vim ./config/SF_connector.properties
# goto: `snowflake.private.key=`
# paste
MIIEowIB...\
...t6G8LG1r
```

copy-paste public key text to SF sql:
```sql
-- use role sysadmin;
use role accountadmin;
use schema ecommerce_db.ecommerce_liv;
alter user vlk set RSA_PUBLIC_KEY='MIIBIj...
...QAB';
desc user vlk;
```

# real-time streaming with kafka https://youtu.be/EQ44K5GfgDw?t=4115

https://docs.snowflake.com/en/user-guide/kafka-connector-overview

steps
- create a topic in kafka
- insert data to topic
- stream data from the topic into snowflake table

create schema for kafka, use SF worksheet:
```sql
-- use role sysadmin;
use role accountadmin;
use database ecommerce_db;
create schema ecommerce_db.kafka_live_streaming;
use schema ecommerce_db.kafka_live_streaming;
```

change topic name in connect properties file:
`vim ./config/SF_connector.properties`
```properties
topics=salesData
snowflake.topic2table.map=salesData:sales_data
```

Create topic:
`bin/kafka-topics.sh --create --topic salesData --bootstrap-server :::9092`

N.b. kafka should be started already:
```s
bin/zookeeper-server-start.sh config/zookeeper.properties &
bin/kafka-server-start.sh config/server.properties &
```

Generate data:
```s
bin/kafka-console-producer.sh --topic salesData --bootstrap-server :::9092
{"name":"John", "age":30, "car":null}
{"name":"Alice", "age":25, "car":"ford"}
```
Check: `bin/kafka-console-consumer.sh --topic salesData --from-beginning --bootstrap-server :::9092`

Push data to snowflake:
```s
bin/connect-standalone.sh ./config/connect-standalone.properties ./config/SF_connector.properties
# errors, see below
```
It should create raw-data table, with 2 fields: (meta, data).
You have to transform raw data to structured data, later.

check (SF worksheet):
```sql
select * from sales_data limit 3;
select * from ECOMMERCE_DB.KAFKA_LIVE_STREAMING.SALES_DATA limit 3;
RECORD_METADATA	RECORD_CONTENT
{
  "CreateTime": 1729691102669,
  "key": {},
  "key_schema_id": 0,
  "offset": 0,
  "partition": 0,
  "topic": "salesData"
}	{
  "age": 30,
  "car": null,
  "name": "John"
}

{
  "CreateTime": 1729691107818,
  "key": {},
  "key_schema_id": 0,
  "offset": 1,
  "partition": 0,
  "topic": "salesData"
}	{
  "age": 25,
  "car": "ford",
  "name": "Alice"
}
```

Kafka Connector errors:
```
bin/connect-standalone.sh ./config/connect-standalone.properties ./config/SF_connector.properties
...
org.apache.kafka.connect.errors.ConnectException: Failed to find any class that implements Connector and which name matches com.snowflake.kafka.connector.SnowflakeSinkConnector, available connectors are: ...

ok, goto docs: https://docs.snowflake.com/en/user-guide/kafka-connector

> The Kafka connector is designed to run in a Kafka Connect cluster to read data from Kafka topics and write the data into Snowflake tables
The Kafka connector supports two data loading methods:
- Snowpipe
- Snowpipe Streaming.
With Snowpipe Streaming, the Kafka connector optionally supports schema detection and evolution.
...
Each Kafka message is passed to Snowflake in JSON format or Avro format. 
...
each topic should be processed by only one instance of the connector
...
There is no guarantee that rows are inserted in the order that they were originally published

https://mvnrepository.com/artifact/com.snowflake/snowflake-kafka-connector/2.4.1
copy jar to kafka_2.13-3.8.0/libs/snowflake-kafka-connector-2.4.1.jar

Next try
bin/connect-standalone.sh ./config/connect-standalone.properties ./config/SF_connector.properties
...
error: [SF_KAFKA_CONNECTOR] Exception: Failed to prepare SQL statement
Error Code: 2001
Detail: SQL Exception, reported by Snowflake JDBC
Message: SQL compilation error:
Stage 'ECOMMERCE_DB.KAFKA_LIVE_STREAMING.SNOWFLAKE_KAFKA_CONNECTOR_KAFKA_LIVE_STREAMING_STAGE_SALES_DATA' does not exist or not authorized.

Stop right here: I mixed up topic names, 'testData' vs 'salesData'.
After fixing this, all works as expected.

ok, goto docs: https://docs.snowflake.com/en/user-guide/kafka-connector-install#creating-a-role-to-use-the-kafka-connector

-- Use a role that can create and manage roles and privileges.
USE ROLE securityadmin;

-- Create a Snowflake role with the privileges to work with the connector.
CREATE ROLE kc_role_1;

-- Grant privileges on the database.
GRANT USAGE ON DATABASE ecommerce_db TO ROLE kc_role_1;

-- Grant privileges on the schema.
-- use database ecommerce_db;
GRANT USAGE ON SCHEMA ecommerce_db.kafka_live_streaming TO ROLE kc_role_1;
GRANT CREATE TABLE ON SCHEMA ecommerce_db.kafka_live_streaming TO ROLE kc_role_1;
GRANT CREATE STAGE ON SCHEMA ecommerce_db.kafka_live_streaming TO ROLE kc_role_1;
GRANT CREATE PIPE ON SCHEMA ecommerce_db.kafka_live_streaming TO ROLE kc_role_1;

-- Only required if the Kafka connector will load data into an existing table.
-- GRANT OWNERSHIP ON TABLE existing_table1 TO ROLE kc_role_1;

-- Only required if the Kafka connector will stage data files in an existing internal stage: (not recommended).
-- GRANT READ, WRITE ON STAGE existing_stage1 TO ROLE kc_role_1;

-- Grant the custom role to an existing user.
GRANT ROLE kc_role_1 TO USER vlk;

-- Set the custom role as the default role for the user.
-- If you encounter an 'Insufficient privileges' error, verify the role that has the OWNERSHIP privilege on the user.
ALTER USER vlk SET DEFAULT_ROLE = kc_role_1; -- current: ACCOUNTADMIN

Next try:
bin/connect-standalone.sh ./config/connect-standalone.properties ./config/SF_connector.properties
...
the same error:
Stage 'ECOMMERCE_DB.KAFKA_LIVE_STREAMING.SNOWFLAKE_KAFKA_CONNECTOR_KAFKA_LIVE_STREAMING_STAGE_SALES_DATA' does not exist or not authorized.

Maybe create stage by hand?
use role accountadmin;
CREATE STAGE ECOMMERCE_DB.KAFKA_LIVE_STREAMING.SNOWFLAKE_KAFKA_CONNECTOR_KAFKA_LIVE_STREAMING_STAGE_SALES_DATA FILE_FORMAT = (TYPE = JSON);
GRANT READ, WRITE ON STAGE ECOMMERCE_DB.KAFKA_LIVE_STREAMING.SNOWFLAKE_KAFKA_CONNECTOR_KAFKA_LIVE_STREAMING_STAGE_SALES_DATA TO ROLE kc_role_1;
show stages;

```

next: zero copy clone https://youtu.be/EQ44K5GfgDw?t=4318
