import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;

public class TestAsyncLookupTableBySql {

    /**
     * Get  不支持嵌套类型，使用会报错
     */
    @Test
    @Ignore
    public void testGetFailOnNestedType() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);
        tEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        tEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20));
        tEnv.getConfig().getConfiguration().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tEnv.executeSql("CREATE TABLE source (\n" +
                "    test_bigint BIGINT,\n" +
                "    test_decimal        DECIMAL(2,2),\n" +
                "    order_time   AS PROCTIME (),\n" +
                "    test_float float," +
                "    test_string string, " +
                "    test_row row<a string>" +
                ") WITH (\n" +
                "  'connector' = 'datagen'\n" +
                ")");
        tEnv.executeSql("describe  source").print();
        tEnv.executeSql("CREATE TABLE dim (\n" +
                " test_bigint BIGINT,\n" +
                " test_decimal DECIMAL(2,2) , " +
                " test_float float, " +
                " test_string string," +
                " test_row row<a string>," +
                " copy_time bigint" +
                ") WITH (\n" +
                "   'connector' = 'http_table',\n" +
                "   'url' = 'http://127.0.0.1/test',\n" +
                "   'request.method' = 'get'\n" +
                ")");
        tEnv.executeSql("describe  dim").print();
        tEnv.executeSql("select *  from source left join dim FOR SYSTEM_TIME AS OF source.order_time " +
                "on  dim.test_bigint = source.test_bigint and  source.test_decimal=dim.test_decimal  and source.test_float=dim.test_float and source.test_string=dim.test_string and source.test_row=dim.test_row").print();
    }


    /**
     * Get 成功请求样式
     */
    @Test
    @Ignore
    public void testGetPass() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);
        tEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        tEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20));
        tEnv.getConfig().getConfiguration().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tEnv.executeSql("CREATE TABLE source (\n" +
                "    test_bigint BIGINT,\n" +
                "    test_decimal        DECIMAL(2,2),\n" +
                "    order_time   AS PROCTIME (),\n" +
                "    test_float float," +
                "    test_string string " +
                ") WITH (\n" +
                "  'connector' = 'datagen'\n" +
                ")");
        tEnv.executeSql("describe  source").print();
        tEnv.executeSql("CREATE TABLE dim (\n" +
                " test_bigint BIGINT,\n" +
                " test_decimal DECIMAL(2,2) , " +
                " test_float float, " +
                " test_string string," +
                " copy_time bigint" +
                ") WITH (\n" +
                "   'connector' = 'http_table',\n" +
                "   'url' = 'http://127.0.0.1/test',\n" +
                "   'request.method' = 'get'\n" +
                ")");
        tEnv.executeSql("describe  dim").print();
        tEnv.executeSql("select *  from source left join dim FOR SYSTEM_TIME AS OF source.order_time " +
                "on  dim.test_bigint = source.test_bigint and  source.test_decimal=dim.test_decimal  and source.test_float=dim.test_float and source.test_string=dim.test_string and source.test_row=dim.test_row").print();
    }

    /**
     * Post 请求成功样式
     */
    @Test
    @Ignore
    public void testPostPass() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);
        tEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        tEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20));
        tEnv.getConfig().getConfiguration().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tEnv.getConfig().getConfiguration().setString("table.exec.resource.default-parallelism", "1");
        tEnv.executeSql("CREATE TABLE source (\n" +
                "    test_bigint BIGINT,\n" +
                "    test_decimal        DECIMAL(2,2),\n" +
                "    order_time   AS PROCTIME (),\n" +
                "    test_float float," +
                "    test_string string, " +
                "    test_row row<a string>" +
                ") WITH (\n" +
                "  'connector' = 'datagen'\n" +
                ")");
        tEnv.executeSql("describe  source").print();
        tEnv.executeSql("CREATE TABLE dim (\n" +
                " test_bigint BIGINT,\n" +
                " test_decimal DECIMAL(2,2) , " +
                " test_float float, " +
                " test_string string," +
                " test_row row<a string>," +
                " copy_time bigint" +
                ") WITH (\n" +
                "   'connector' = 'http_table',\n" +
                "   'url' = 'http://127.0.0.1/test',\n" +
                "   'request.method' = 'post'\n" +
                ")");
        tEnv.executeSql("describe  dim").print();
        tEnv.executeSql("select *  from source left join dim FOR SYSTEM_TIME AS OF source.order_time " +
                "on  dim.test_bigint = source.test_bigint and  source.test_decimal=dim.test_decimal  and source.test_float=dim.test_float and source.test_string=dim.test_string and source.test_row=dim.test_row").print();
    }

}