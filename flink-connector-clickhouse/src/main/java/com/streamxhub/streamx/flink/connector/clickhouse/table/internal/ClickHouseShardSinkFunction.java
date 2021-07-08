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
package com.streamxhub.streamx.flink.connector.clickhouse.table.internal;

import com.streamxhub.streamx.flink.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.streamxhub.streamx.flink.connector.clickhouse.table.internal.converter.ClickHouseRowConverter;
import com.streamxhub.streamx.flink.connector.clickhouse.table.internal.executor.ClickHouseBatchExecutor;
import com.streamxhub.streamx.flink.connector.clickhouse.table.internal.executor.ClickHouseExecutor;
import com.streamxhub.streamx.flink.connector.clickhouse.table.internal.executor.ClickHouseUpsertExecutor;
import com.streamxhub.streamx.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import com.streamxhub.streamx.flink.connector.clickhouse.table.internal.partitioner.ClickHousePartitioner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author benjobs
 */
public class ClickHouseShardSinkFunction extends AbstractClickHouseSinkFunction {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseShardSinkFunction.class);

    private static final Pattern PATTERN = Pattern.compile("Distributed\\((?<cluster>[a-zA-Z_][0-9a-zA-Z_]*),\\s*(?<database>[a-zA-Z_][0-9a-zA-Z_]*),\\s*(?<table>[a-zA-Z_][0-9a-zA-Z_]*)");

    private final ClickHouseConnectionProvider connectionProvider;

    private final ClickHouseRowConverter converter;

    private final ClickHousePartitioner partitioner;

    private final ClickHouseOptions options;

    private final String[] fieldNames;

    private transient boolean closed = false;

    private transient Connection connection;

    private String remoteTable;

    private transient Collection<Connection> shardConnections;

    private transient int[] batchCounts;

    private final List<ClickHouseExecutor> shardExecutors;

    private final String[] keyFields;

    private final boolean ignoreDelete;

    protected ClickHouseShardSinkFunction(@Nonnull ClickHouseConnectionProvider connectionProvider,
                                          @Nonnull String[] fieldNames,
                                          @Nonnull Optional<String[]> keyFields,
                                          @Nonnull ClickHouseRowConverter converter,
                                          @Nonnull ClickHousePartitioner partitioner,
                                          @Nonnull ClickHouseOptions options) {
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
        this.fieldNames = Preconditions.checkNotNull(fieldNames);
        this.converter = Preconditions.checkNotNull(converter);
        this.partitioner = Preconditions.checkNotNull(partitioner);
        this.options = Preconditions.checkNotNull(options);
        this.shardExecutors = new ArrayList<>();
        this.ignoreDelete = options.getIgnoreDelete();
        this.keyFields = keyFields.orElseGet(() -> new String[0]);
    }

    @Override
    public void open(Configuration parameters) throws IOException {
        try {
            this.connection = this.connectionProvider.getConnection();
            establishShardConnections();
            initializeExecutors();
        } catch (Exception e) {
            throw new IOException("unable to establish connection to ClickHouse", e);
        }
    }

    private void establishShardConnections() throws IOException {
        try {
            String engine = this.connectionProvider.queryTableEngine(this.options.getDatabaseName(), this.options.getTableName());
            Matcher matcher = PATTERN.matcher(engine);
            if (matcher.find()) {
                String remoteCluster = matcher.group("cluster");
                String remoteDatabase = matcher.group("database");
                this.remoteTable = matcher.group("table");
                this.shardConnections = this.connectionProvider.getShardConnections(remoteCluster, remoteDatabase);
                this.batchCounts = new int[this.shardConnections.size()];
            } else {
                throw new IOException("table `" + this.options.getDatabaseName() + "`.`" + this.options.getTableName() + "` is not a Distributed table");
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private void initializeExecutors() throws SQLException {
        String sql = ClickHouseStatementFactory.getInsertIntoStatement(this.remoteTable, this.fieldNames);
        for (Connection shardConnection : this.shardConnections) {
            if (this.keyFields.length > 0) {
                ClickHouseUpsertExecutor clickHouseUpsertExecutor = ClickHouseExecutor.createUpsertExecutor(this.remoteTable, this.fieldNames, this.keyFields, this.converter, this.options);
                clickHouseUpsertExecutor.prepareStatement(shardConnection);
                this.shardExecutors.add(clickHouseUpsertExecutor);
            } else {
                ClickHouseBatchExecutor clickHouseBatchExecutor = new ClickHouseBatchExecutor(sql, this.converter, this.options.getFlushInterval(), this.options.getBatchSize(), this.options.getMaxRetries(), null);
                clickHouseBatchExecutor.prepareStatement(shardConnection);
                this.shardExecutors.add(clickHouseBatchExecutor);
            }
        }
    }

    @Override
    public void invoke(RowData record, Context context) throws IOException {
        switch (record.getRowKind()) {
            case INSERT:
                writeRecordToOneExecutor(record);
                return;
            case UPDATE_AFTER:
                if (this.ignoreDelete) {
                    writeRecordToOneExecutor(record);
                } else {
                    writeRecordToAllExecutors(record);
                }
                return;
            case DELETE:
                if (!this.ignoreDelete) {
                    writeRecordToAllExecutors(record);
                }
                return;
            case UPDATE_BEFORE:
                return;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE, but get: %s.", record.getRowKind()));
        }
    }

    private void writeRecordToOneExecutor(RowData record) throws IOException {
        int selected = this.partitioner.select(record, this.shardExecutors.size());
        this.shardExecutors.get(selected).addBatch(record);
        this.batchCounts[selected] = this.batchCounts[selected] + 1;
        if (this.batchCounts[selected] >= this.options.getBatchSize()) {
            flush(selected);
        }
    }

    private void writeRecordToAllExecutors(RowData record) throws IOException {
        for (int i = 0; i < this.shardExecutors.size(); i++) {
            this.shardExecutors.get(i).addBatch(record);
            this.batchCounts[i] = this.batchCounts[i] + 1;
            if (this.batchCounts[i] >= this.options.getBatchSize()) {
                flush(i);
            }
        }
    }

    @Override
    public void flush() throws IOException {
        for (int i = 0; i < this.shardExecutors.size(); i++) {
            flush(i);
        }
    }

    public void flush(int index) throws IOException {
        this.shardExecutors.get(index).executeBatch();
        this.batchCounts[index] = 0;
    }

    @Override
    public void close() {
        if (!this.closed) {
            this.closed = true;
            try {
                flush();
            } catch (Exception e) {
                LOG.warn("Writing records to ClickHouse failed.", e);
            }
            closeConnection();
        }
    }

    private void closeConnection() {
        if (this.connection != null) {
            try {
                for (ClickHouseExecutor shardExecutor : this.shardExecutors) {
                    shardExecutor.closeStatement();
                }
                this.connectionProvider.closeConnection();
            } catch (SQLException se) {
                LOG.warn("ClickHouse connection could not be closed: {}", se.getMessage());
            } finally {
                this.connection = null;
            }
        }
    }
}
