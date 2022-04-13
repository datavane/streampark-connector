package com.streamxhub.streamx.flink.connector.pulsar;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Test;

/**
 * @author DarrenDa
 * @version 1.0
 * @Desc: Test case
 */
public class PulsarSqlCase {

    @Test
    public void mytest() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        tableEnvironment.executeSql("" +
                "CREATE TABLE source_pulsar_n(\n" +
                "    requestId VARCHAR,\n" +
                "    `timestamp` BIGINT,\n" +
                "    `date` VARCHAR,\n" +
                "    appId VARCHAR,\n" +
                "    appName VARCHAR,\n" +
                "    forwardTimeMs VARCHAR,\n" +
                "    processingTimeMs INT,\n" +
                "    errCode VARCHAR,\n" +
                "    userIp VARCHAR,\n" +
                "    createTime bigint,\n" +
                "    b_create_time as TO_TIMESTAMP(FROM_UNIXTIME(createTime/1000,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss')\n" +
                ") WITH (\n" +
                "  'connector.type' = 'pulsar',\n" +
                "  'connector.version' = 'universal',\n" +
                "  'connector.topic' = 'persistent://streamx/dev/context.pulsar',\n" +
                "  'connector.service-url' = 'pulsar://pulsar-streamx-n.stream.com:6650',\n" +
                "  'connector.subscription-name' = 'tmp_print_detail',\n" +
                "  'connector.subscription-type' = 'Shared',\n" +
                "  'connector.subscription-initial-position' = 'Latest',\n" +
                "  'update-mode' = 'append',\n" +
                "  'format.type' = 'json',\n" +
                "  'format.derive-schema' = 'true'\n" +
                ")" )
                ;

        tableEnvironment.executeSql("" +
                "create table sink_pulsar_result(\n" +
                "    requestId VARCHAR,\n" +
                "    `timestamp` BIGINT,\n" +
                "    `date` VARCHAR,\n" +
                "    appId VARCHAR,\n" +
                "    appName VARCHAR,\n" +
                "    forwardTimeMs VARCHAR,\n" +
                "    processingTimeMs INT,\n" +
                "    errCode VARCHAR,\n" +
                "    userIp VARCHAR\n" +
                ") with (\n" +
                "  'connector' = 'print'\n" +
                ")");

        tableEnvironment.executeSql("" +
                "insert into sink_pulsar_result\n" +
                "select \n" +
                "      requestId ,\n" +
                "      `timestamp`,\n" +
                "      `date`,\n" +
                "      appId,\n" +
                "      appName,\n" +
                "      forwardTimeMs,\n" +
                "      processingTimeMs,\n" +
                "      errCode,\n" +
                "      userIp,\n" +
                "      b_create_time\n" +
                "from source_pulsar_n");

        Table tb = tableEnvironment.sqlQuery("select * from sink_pulsar_result");

        tb.execute().print();
    }
}
