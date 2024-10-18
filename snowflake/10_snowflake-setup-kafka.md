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
- config Connect-standalone.properties file

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

next: setup encription https://youtu.be/EQ44K5GfgDw?t=3986
