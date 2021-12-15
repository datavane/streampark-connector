## flink-connector-clickhouse

> 采用[clickhouse-jdbc](https://github.com/ClickHouse/clickhouse-jdbc) 官方jdbc驱动实现,支持Array类型写入

## 🚀 快速上手
```shell
git clone https://github.com/streamxhub/streamx-connector.git
cd streamx-connector/flink-connector-clickhouse
mvn clean install -DskipTests -Dflink.version=$version
```

## 🎉 Features
* 支持批量写入定时刷新
* 支持写入集群表和本地表
* 支持三种写入策略(hash | shuffle | balanced)
* 支持Clickhouse Array类型写入
* 支持Clickhouse Map类型写入
* 
## 👻 使用

```sql
create TABLE log_detail_source(
    requestId VARCHAR,
    `timestamp` BIGINT,
    `date` VARCHAR,
    appId VARCHAR,
    appName VARCHAR,
    forwardTimeMs VARCHAR,
    processingTimeMs INT,
    errCode VARCHAR,
    userIp VARCHAR,
    accountIdList ARRAY<VARCHAR>,
    properties MAP<STRING,STRING>
) WITH (
    'connector.type' = 'kafka',
    'connector.version' = 'universal',
    'connector.properties.group.id' = 'log_detail',
    'connector.properties.bootstrap.servers' = '10.10.10.1:9092,10.10.10.2:9092,10.10.10.2:9092',
    'connector.topic' = 'log_detail',
    'connector.startup-mode' = 'earliest-offset',
    'format.type' = 'json'
)

create TABLE log_detail_sink(
    requestId VARCHAR,
    `timestamp` BIGINT,
    `date` VARCHAR,
    appId VARCHAR,
    appName VARCHAR,
    forwardTimeMs VARCHAR,
    processingTimeMs INT,
    errCode VARCHAR,
    userIp VARCHAR,
    accountIdList ARRAY<VARCHAR>,
    properties MAP<STRING,STRING>
) WITH (      
    'connector' = 'clickhouse',
    'url' = 'clickhouse://10.10.10.1:8123',
    'database-name' = 'default',
    'table-name' = 'log_detail',
    'username' = 'default',
    'password' = '123456',
    'sink.max-retries' = '3',                 /* 失败重试次数 */
    'sink.batch-size' = '100000',             /* batch 大小 */
    'sink.flush-interval' = '1000',           /* flush 时间间隔 */
    'sink.max-retries' = '1',                 /* 最大重试次数 */
    'sink.partition-strategy' = 'balanced',   /* hash | shuffle | balanced */
    'sink.write-local' = 'true',              /* 如果为ture则默认写入本地表,否则写入集群表*/
    'sink.ignore-delete' = 'true'             /* 忽略 DELETE 并视 UPDATE 为 INSERT */
)

insert into log_detail_sink select * from log_detail_source

```
