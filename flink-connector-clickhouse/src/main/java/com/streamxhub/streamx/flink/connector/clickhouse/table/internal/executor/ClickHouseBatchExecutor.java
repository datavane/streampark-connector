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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.except.ClickHouseException;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @author benjobs
 */
public class ClickHouseBatchExecutor implements ClickHouseExecutor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBatchExecutor.class);

    private transient java.sql.PreparedStatement stmt;

    private TypeInformation<RowData> rowDataTypeInformation;

    private final String sql;

    private final ClickHouseRowConverter converter;

    private transient List<RowData> batch;

    private final Duration flushInterval;

    private final int batchSize;

    private final int maxRetries;

    private transient TypeSerializer<RowData> typeSerializer;

    private boolean objectReuseEnabled = false;

    private transient ExecuteBatchService service;

    public ClickHouseBatchExecutor(String sql,
                                   ClickHouseRowConverter converter,
                                   Duration flushInterval,
                                   int batchSize,
                                   int maxRetries,
                                   TypeInformation<RowData> rowDataTypeInformation) {
        this.sql = sql;
        this.converter = converter;
        this.flushInterval = flushInterval;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.rowDataTypeInformation = rowDataTypeInformation;
    }

    @Override
    public void prepareStatement(java.sql.Connection connection) throws SQLException {
        this.batch = new ArrayList<>();
        this.stmt = connection.prepareStatement(this.sql);
        this.service = new ExecuteBatchService();
        this.service.startAsync();
    }

    @Override
    public void prepareStatement(ClickHouseConnectionProvider connectionProvider) throws SQLException {
        this.batch = new ArrayList<>();
        this.stmt = connectionProvider.getConnection().prepareStatement(this.sql);
        this.service = new ExecuteBatchService();
        this.service.startAsync();
    }

    @Override
    public void setRuntimeContext(RuntimeContext context) {
        this.typeSerializer = this.rowDataTypeInformation.createSerializer(context.getExecutionConfig());
        this.objectReuseEnabled = context.getExecutionConfig().isObjectReuseEnabled();
    }

    @Override
    public synchronized void addBatch(RowData rowData) throws IOException {
        if (rowData.getRowKind() != RowKind.DELETE && rowData.getRowKind() != RowKind.UPDATE_BEFORE) {
            if (this.objectReuseEnabled) {
                this.batch.add(this.typeSerializer.copy(rowData));
            } else {
                this.batch.add(rowData);
            }
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

        if (this.stmt != null) {
            this.stmt.close();
            this.stmt = null;
        }
    }

    @Override
    public String getState() {
        return ClickHouseBatchExecutor.this.service.state().toString();
    }

    private class ExecuteBatchService extends AbstractExecutionThreadService {
        private ExecuteBatchService() {
        }

        @Override
        protected void run() throws Exception {
            while (isRunning()) {
                synchronized (ClickHouseBatchExecutor.this) {
                    ClickHouseBatchExecutor.this.wait(ClickHouseBatchExecutor.this.flushInterval.toMillis());
                    if (!ClickHouseBatchExecutor.this.batch.isEmpty()) {
                        for (RowData rowData : ClickHouseBatchExecutor.this.batch) {
                            try{
                                ClickHouseBatchExecutor.this.converter.toClickHouse(rowData, ClickHouseBatchExecutor.this.stmt);
                                ClickHouseBatchExecutor.this.stmt.addBatch();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        attemptExecuteBatch();
                    }
                }
            }
        }

        private void attemptExecuteBatch() throws Exception {
            for (int index = 1; index <= ClickHouseBatchExecutor.this.maxRetries; index++) {
                try {
                    ClickHouseBatchExecutor.this.stmt.executeBatch();
                    ClickHouseBatchExecutor.this.batch.clear();
                    break;
                } catch (ClickHouseException exception) {
                    ClickHouseBatchExecutor.LOG.error("ClickHouse error", exception);
                    //当出现ClickHouse exception, code: 27 ...DB::Exception: Cannot parse input 即这条数据是错误时,略过此次插入
                    int errorCode = exception.getErrorCode();
                    if (errorCode == 27) {
                        ClickHouseBatchExecutor.this.stmt.clearBatch();
                        ClickHouseBatchExecutor.this.batch.clear();
                        break;
                    }
                } catch (SQLException sqlException) {
                    LOG.error("ClickHouse executeBatch error, retry times = {}", index, sqlException);
                    if (index >= ClickHouseBatchExecutor.this.maxRetries) {
                        throw new IOException(sqlException);
                    }
                    try {
                        Thread.sleep((1000L * index));
                    } catch (InterruptedException interruptedException) {
                        Thread.currentThread().interrupt();
                        throw new IOException("unable to flush; interrupted while doing another attempt", interruptedException);
                    }
                }
            }
        }
    }
}
