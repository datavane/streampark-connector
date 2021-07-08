/*
 * Copyright (c) 2019 The StreamX Project
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.streamxhub.streamx.flink.connector.clickhouse.table;

import com.streamxhub.streamx.flink.connector.clickhouse.table.internal.AbstractClickHouseSinkFunction;
import com.streamxhub.streamx.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

/**
 * @author benjobs
 */
public class ClickHouseDynamicTableSink implements DynamicTableSink {
    private final TableSchema tableSchema;

    private final ClickHouseOptions options;

    public ClickHouseDynamicTableSink(TableSchema tableSchema, ClickHouseOptions options) {
        this.tableSchema = tableSchema;
        this.options = options;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        validatePrimaryKey(requestedMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        Preconditions.checkState((ChangelogMode.insertOnly().equals(requestedMode) || this.tableSchema.getPrimaryKey().isPresent()), "please declare primary key for sink table when query contains update/delete record.");
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        AbstractClickHouseSinkFunction sinkFunction =
                (new AbstractClickHouseSinkFunction.Builder())
                        .withOptions(this.options)
                        .withFieldNames(this.tableSchema.getFieldNames())
                        .withFieldDataTypes(this.tableSchema.getFieldDataTypes())
                        .withPrimaryKey(this.tableSchema.getPrimaryKey())
                        .withRowDataTypeInfo(context.createTypeInformation(this.tableSchema.toRowDataType()))
                        .build();
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public ClickHouseDynamicTableSink copy() {
        return new ClickHouseDynamicTableSink(this.tableSchema, this.options);
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse";
    }
}
