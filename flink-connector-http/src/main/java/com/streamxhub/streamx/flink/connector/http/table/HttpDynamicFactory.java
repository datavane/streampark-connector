package com.streamxhub.streamx.flink.connector.http.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 * @author Whojohn
 */
public class HttpDynamicFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    public static final ConfigOption<String> URL = ConfigOptions
            .key("url")
            .stringType()
            .noDefaultValue()
            .withDescription("Request url such as: http://test:80 .");

    public static final ConfigOption<String> REQUEST_METHOD = ConfigOptions
            .key("request.method")
            .stringType()
            .defaultValue("get")
            .withDescription("Request method allow as follow: get , post .");

    public static final ConfigOption<Integer> REQUEST_TIMEOUT = ConfigOptions
            .key("request.timeout")
            .intType()
            .defaultValue(30000)
            .withDescription("Such as : 30000 ( ms)");

    public static final ConfigOption<Integer> REQUEST_RETRY_MAX = ConfigOptions
            .key("request.retry.max")
            .intType()
            .defaultValue(3)
            .withDescription("Such as : 3 (Retry when fail request 3 time.); Notice target request port close will also retry.");

    public static final ConfigOption<Integer> REQUEST_RETRY_INTERVAL = ConfigOptions
            .key("request.max.interval")
            .intType()
            .defaultValue(1000)
            .withDescription("Such as : 1000 ( ms);  Notice target request port close will also retry.");


    public static final ConfigOption<Integer> REQUEST_THREAD = ConfigOptions
            .key("request.thread")
            .intType()
            .defaultValue(1)
            .withDeprecatedKeys("IoReactor,MaxThreadPerChannel,MaxConnThread Request thread.");


    public static final ConfigOption<Integer> REQUEST_LIMIT_GAP = ConfigOptions
            .key("request.limit.gap")
            .intType()
            .defaultValue(3000)
            .withDescription("Request limit each slot ,default is 3000 .");


    public static final ConfigOption<Integer> REQUEST_LIMIT_SLEEP_MS = ConfigOptions
            .key("request.limit.sleep.ms")
            .intType()
            .defaultValue(3000)
            .withDescription("Request limit each slot ,default is 3000 .");


    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig options = helper.getOptions();
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        Optional<Duration> temp = context.getConfiguration().getOptional(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL);
        Long sec = !temp.isPresent() ? 60:temp.get().getSeconds();
        return new HttpDynamicTableLookupSource(options,
                schema,
                sec);
    }

    @Override
    public String factoryIdentifier() {
        return "http_table";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(REQUEST_METHOD);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(REQUEST_LIMIT_GAP);
        optionalOptions.add(REQUEST_LIMIT_SLEEP_MS);
        optionalOptions.add(REQUEST_THREAD);
        optionalOptions.add(REQUEST_TIMEOUT);
        optionalOptions.add(REQUEST_RETRY_INTERVAL);
        optionalOptions.add(REQUEST_RETRY_MAX);
        return optionalOptions;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        throw new UnsupportedOperationException("Don't support http sink now.");
    }
}
