## flink-connector-pulsar

> 概要说明：
> 实现依附：https://gitee.com/apache/flink/tree/release-1.14/flink-connectors
* Flink 官方自1.14版本支持 Flink-pulsar-connector(目前未支持 Flink-sql)
* 在此版本前，自主实现了Flink-pulsar-connector，本次Flink-sql的实现向官方Flink-connector-pulsar对齐，更好的兼容使用，实现性能最优！
* 就生产经验，避坑处理
* 本次Pulsar版本使用版本：2.8.2  Flink版本：1.14.3

## ★详情介绍 Pulsar-SQL Connector
### Dependencies
In order to use the Pulsar connector the following dependencies are required for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

* Maven dependency

```
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-Pulsar_2.11</artifactId>
  <version>1.14.3</version>
</dependency>
```
### How to create a Pulsar table
```
CREATE TABLE source_pulsar_n(
    requestId VARCHAR,
    `timestamp` BIGINT,
    `date` VARCHAR,
    appId VARCHAR,
    appName VARCHAR,
    forwardTimeMs VARCHAR,
    processingTimeMs INT,
    errCode VARCHAR,
    userIp VARCHAR,
    b_create_time as TO_TIMESTAMP(FROM_UNIXTIME(createTime/1000,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss')
) WITH (
  'connector.type' = 'pulsar',
  'connector.version' = 'universal',
  'connector.topic' = 'persistent://streamx/dev/context.pulsar',
  'connector.service-url' = 'pulsar://pulsar-streamx-n.stream.com:6650',
  'connector.subscription-name' = 'tmp_print_detail',
  'connector.subscription-type' = 'Shared',
  'connector.subscription-initial-position' = 'Latest',
  'update-mode' = 'append',
  'format.type' = 'json',
  'format.derive-schema' = 'true'
);
```
### Data Type Mapping
Pulsar stores message keys and values as bytes, so Pulsar doesn’t have schema or data types. The Pulsar messages are deserialized and serialized by formats, e.g. csv, json, avro. Thus, the data type mapping is determined by specific formats. Please refer to Formats pages for more details.

### Connector Options
| Option                                  | Required          | Default | Type   | Description                                                  |
| --------------------------------------- | ----------------- | ------- | ------ | ------------------------------------------------------------ |
| connector.type                          | required          | (none)  | String | Specify what connector to use, for pulsar use `'pulsar'`.      |
| connector.version                       | required          | (none)  | String | universal                                                    |
| connector.topic                         | required for sink | (none)  | String | Topic name(s) to read data from when the table is used as source |
| connector.service-url                   | optional          | (none)  | String | The address of the pulsar                                    |
| connector.subscription-name             | required          | (none)  | String | The subscription name of the Pulsar                          |
| connector.subscription-type             | required          | (none)  | String | A subscription model of the Pulsar【Shared、Exclusive、Key_Shared、Failover】 |
| connector.subscription-initial-position | required          | (none)  | String | initial-position[EARLIEST、LATEST、TIMESTAMP]                |
| update-mode                             | optional          | (none)  | String | append or upsert                                             |
| format.type                             | optional          | (none)  | String | json、csv......                                              |
| format.derive-schema                    | optional          | (none)  | String | ture or false                                                |
|                                         |                   |         |        |                                                              |





## 🚀 快速上手
```shell
git clone https://github.com/streamxhub/streamx-connector.git
cd streamx-connector/flink-connector-pulsar
mvn clean install -DskipTests -Dflink.version=$version
```

## 🎉 Features

* Key and Value Formats 

Both the key and value part of a Pulsar record can be serialized to and deserialized from raw bytes using one of the given
  
* Value Format

Since a key is optional in Pulsar records, the following statement reads and writes records with a configured value format but without a key format. The 'format' option is a synonym for 'value.format'. All format options are prefixed with the format identifier.

## 👻 使用

```sql
-- Pulsar多集群形式，
-- 此处分 n、b 两个集群

--声明数据源
CREATE TABLE source_pulsar_n(
    requestId VARCHAR,
    `timestamp` BIGINT,
    `date` VARCHAR,
    appId VARCHAR,
    appName VARCHAR,
    forwardTimeMs VARCHAR,
    processingTimeMs INT,
    errCode VARCHAR,
    userIp VARCHAR,
    createTime BIGINT,
    b_create_time as TO_TIMESTAMP(FROM_UNIXTIME(createTime/1000,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss')
) WITH (
  'connector.type' = 'pulsar',
  'connector.version' = 'universal',
  'connector.topic' = 'persistent://streamx/dev/context.pulsar',
  'connector.service-url' = 'pulsar://pulsar-streamx-n.stream.com:6650',
  'connector.subscription-name' = 'tmp_print_detail',
  'connector.subscription-type' = 'Shared',
  'connector.subscription-initial-position' = 'Latest',
  'update-mode' = 'append',
  'format.type' = 'json',
  'format.derive-schema' = 'true'
);


CREATE TABLE source_pulsar_b(
    requestId VARCHAR,
    `timestamp` BIGINT,
    `date` VARCHAR,
    appId VARCHAR,
    appName VARCHAR,
    forwardTimeMs VARCHAR,
    processingTimeMs INT,
    errCode VARCHAR,
    userIp VARCHAR,
    createTime BIGINT,
  b_create_im_time as TO_TIMESTAMP(FROM_UNIXTIME(createTime/1000,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss')
) WITH (
  'connector.type' = 'pulsar',
  'connector.version' = 'universal',
  'connector.topic' = 'persistent://streamx/dev/context.pulsar',
  'connector.service-url' = 'pulsar://pulsar-streamx-b.stream.com:6650',
  'connector.subscription-name' = 'tmp_print_detail',
  'connector.subscription-type' = 'Shared',
  'connector.subscription-initial-position' = 'Latest',
  'update-mode' = 'append',
  'format.type' = 'json',
  'format.derive-schema' = 'true'
);

-- 合并数据源
create view pulsar_source_all AS
select
      requestId ,
      `timestamp`,
      `date`,
      appId,
      appName,
      forwardTimeMs,
      processingTim,
      errCode,
      userIp,
      b_create_time
from source_pulsar_n
union all
select
      requestId ,
      `timestamp`,
      `date`,
      appId,
      appName,
      forwardTimeMs,
      processingTim,
      errCode,
      userIp,
      b_create_time
from source_pulsar_b;

-- 创建 sink
create table sink_pulsar_result(
    requestId VARCHAR,
    `timestamp` BIGINT,
    `date` VARCHAR,
    appId VARCHAR,
    appName VARCHAR,
    forwardTimeMs VARCHAR,
    processingTimeMs INT,
    errCode VARCHAR,
    userIp VARCHAR
) with (
  'connector' = 'print'
);

-- 执行逻辑
-- 查看 pulsar主题明细数据
insert into sink_pulsar_result
select 
      requestId ,
      `timestamp`,
      `date`,
      appId,
      appName,
      forwardTimeMs,
      processingTim,
      errCode,
      userIp,
      b_create_time
from pulsar_source_all;

```
