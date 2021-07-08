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
package com.streamxhub.streamx.flink.connector.clickhouse.table.internal.executor;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.streamxhub.streamx.flink.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.streamxhub.streamx.flink.connector.clickhouse.table.internal.converter.ClickHouseRowConverter;
import com.streamxhub.streamx.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author benjobs
 */
public class ClickHouseUpsertExecutor implements ClickHouseExecutor {
    private static final long serialVersionUID = 1l;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseUpsertExecutor.class);

    private transient PreparedStatement insertStmt;

    private transient PreparedStatement updateStmt;

    private transient PreparedStatement deleteStmt;

    private final String insertSql;

    private final String updateSql;

    private final String deleteSql;

    private final ClickHouseRowConverter converter;

    private final transient List<RowData> insertBatch;

    private final transient List<RowData> updateBatch;

    private final transient List<RowData> deleteBatch;

    private transient ExecuteBatchService service;

    private final Duration flushInterval;

    private final int maxRetries;

    public ClickHouseUpsertExecutor(String insertSql, String updateSql, String deleteSql, ClickHouseRowConverter converter, ClickHouseOptions options) {
        this.insertSql = insertSql;
        this.updateSql = updateSql;
        this.deleteSql = deleteSql;
        this.converter = converter;
        this.flushInterval = options.getFlushInterval();
        this.maxRetries = options.getMaxRetries();
        this.insertBatch = new ArrayList<>();
        this.updateBatch = new ArrayList<>();
        this.deleteBatch = new ArrayList<>();
    }


    @Override
    public void prepareStatement(Connection connection) throws SQLException {
        this.insertStmt = connection.prepareStatement(this.insertSql);
        this.updateStmt = connection.prepareStatement(this.updateSql);
        this.deleteStmt = connection.prepareStatement(this.deleteSql);
        this.service = new ExecuteBatchService();
        this.service.startAsync();
    }

    @Override
    public void prepareStatement(ClickHouseConnectionProvider clickHouseConnectionProvider) throws SQLException {
    }

    @Override
    public void setRuntimeContext(RuntimeContext context) {
    }

    @Override
    public void addBatch(RowData rowData) throws IOException {
        switch (rowData.getRowKind()) {
            case INSERT:
                this.insertBatch.add(rowData);
                break;
            case DELETE:
                this.deleteBatch.add(rowData);
                break;
            case UPDATE_AFTER:
                this.updateBatch.add(rowData);
                break;
            case UPDATE_BEFORE:
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE, but get: %s.", new Object[]{rowData.getRowKind()}));
        }
    }

    @Override
    public synchronized void executeBatch() throws IOException {
        if (this.service.isRunning()) {
            notifyAll();
        } else {
            throw new IOException("executor unexpectedly terminated", this.service.failureCause());
        }
    }

    @Override
    public void closeStatement() throws SQLException {
        if (this.service != null) {
            this.service.stopAsync().awaitTerminated();
        } else {
            LOG.warn("executor closed before initialized");
        }

        List<PreparedStatement> PreparedStatements = Arrays.asList(this.insertStmt, this.updateStmt, this.deleteStmt);
        for (PreparedStatement stmt : PreparedStatements) {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    @Override
    public String getState() {
        return ClickHouseUpsertExecutor.this.service.state().toString();
    }

    private class ExecuteBatchService extends AbstractExecutionThreadService {

        private ExecuteBatchService() {
        }

        @Override
        protected void run() throws Exception {
            while (isRunning()) {
                synchronized (ClickHouseUpsertExecutor.this) {
                    ClickHouseUpsertExecutor.this.wait(ClickHouseUpsertExecutor.this.flushInterval.toMillis());
                    processBatch(ClickHouseUpsertExecutor.this.insertStmt, ClickHouseUpsertExecutor.this.insertBatch);
                    processBatch(ClickHouseUpsertExecutor.this.updateStmt, ClickHouseUpsertExecutor.this.updateBatch);
                    processBatch(ClickHouseUpsertExecutor.this.deleteStmt, ClickHouseUpsertExecutor.this.deleteBatch);
                }
            }
        }

        private void processBatch(PreparedStatement stmt, List<RowData> batch) throws Exception {
            if (!batch.isEmpty()) {
                for (RowData rowData : ClickHouseUpsertExecutor.this.insertBatch) {
                    ClickHouseUpsertExecutor.this.converter.toClickHouse(rowData, stmt);
                    stmt.addBatch();
                }
                attemptExecuteBatch(stmt, batch);
            }
        }

        private void attemptExecuteBatch(PreparedStatement stmt, List<RowData> batch) throws Exception {
            for (int i = 0; i < ClickHouseUpsertExecutor.this.maxRetries; i++) {
                try {
                    stmt.executeBatch();
                    batch.clear();
                    break;
                } catch (SQLException e) {
                    ClickHouseUpsertExecutor.LOG.error("ClickHouse executeBatch error, retry times = {}", i, e);
                    try {
                        Thread.sleep(1000L * i);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new IOException("unable to flush; interrupted while doing another attempt", e);
                    }
                }
            }
        }
    }
}
