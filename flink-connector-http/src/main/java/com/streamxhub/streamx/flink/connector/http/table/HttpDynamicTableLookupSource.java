package com.streamxhub.streamx.flink.connector.http.table;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.util.Preconditions;

import java.util.stream.IntStream;

/**
 * @author Whojohn
 */
public class HttpDynamicTableLookupSource implements LookupTableSource {
    private final ReadableConfig options;
    private final TableSchema schema;
    private final Long checkpointSec;

    public HttpDynamicTableLookupSource(ReadableConfig options, TableSchema schema, long checkpointSec) {
        this.options = options;
        this.schema = schema;
        this.checkpointSec = checkpointSec;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        int[][] contextKeys = context.getKeys();

        String[] keyNames = new String[contextKeys.length];
        // CommonLookupJoin not support nested type which will case context.len = 0 or context[each].len = 0
        // CommonLookupJoin 实现类对于嵌套类型支持有问题，禁止 context.len =0 或者 context[each].len =0 的产生
        Preconditions.checkArgument(contextKeys.length != 0, "So far flink 1.12 not support nested params such as: table a (row<i int>) , a.i =xxx not work, but a =c is ok!!! ;Although LookupContext interface support nested type, CommonLookupJoin instance class not support.");

        IntStream.range(0, keyNames.length).forEach(i -> {
            Preconditions.checkArgument(contextKeys[i].length == 1, "So far flink 1.12 not support nested params such as: table a (row<i int>) , a.i =xxx not work, but a =c is ok!!! ;Although LookupContext interface support nested type, CommonLookupJoin instance class not support.");
            keyNames[i] = schema.getFieldNames()[contextKeys[i][0]];
        });

        return AsyncTableFunctionProvider.of(new HttpAsyncTableFunction(options, schema.getFieldNames(), schema.getFieldDataTypes(), keyNames, checkpointSec,true));
    }

    @Override
    public DynamicTableSource copy() {
        return new HttpDynamicTableLookupSource(this.options, this.schema,this.checkpointSec);
    }

    @Override
    public String asSummaryString() {
        return "Only support Http Post/Get async lookup table.";
    }

//    @Override
//    public ChangelogMode getChangelogMode() {
//        throw new UnsupportedOperationException("Don't support http source now.");
//    }
//
//    @Override
//    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
//        throw new UnsupportedOperationException("Don't support source now.");
//    }
}
