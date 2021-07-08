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
import com.streamxhub.streamx.flink.connector.clickhouse.table.internal.executor.ClickHouseExecutor;
import com.streamxhub.streamx.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.sql.Connection;

/**
 * @author benjobs
 */
public class ClickHouseBatchSinkFunction extends AbstractClickHouseSinkFunction {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBatchSinkFunction.class);

    private final ClickHouseConnectionProvider connectionProvider;

    private transient Connection connection;

    private final ClickHouseExecutor executor;

    private final ClickHouseOptions options;

    private transient boolean closed = false;

    private transient int batchCount = 0;

    public ClickHouseBatchSinkFunction(@Nonnull ClickHouseConnectionProvider connectionProvider,
                                       @Nonnull ClickHouseExecutor executor,
                                       @Nonnull ClickHouseOptions options) {
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
        this.executor = Preconditions.checkNotNull(executor);
        this.options = Preconditions.checkNotNull(options);
    }

    @Override
    public void open(Configuration parameters) throws IOException {
        try {
            this.connection = this.connectionProvider.getConnection();
            this.executor.prepareStatement(this.connectionProvider);
            this.executor.setRuntimeContext(getRuntimeContext());
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("unable to establish connection with ClickHouse", e);
        }
    }

    @Override
    public void invoke(RowData rowData, Context context) throws IOException {
        this.executor.addBatch(rowData);
        this.batchCount++;
        if (this.batchCount >= this.options.getBatchSize()) {
            flush();
        }
    }

    @Override
    public void flush() throws IOException {
        this.executor.executeBatch();
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
                this.executor.closeStatement();
                this.connectionProvider.closeConnection();
            } catch (Exception e) {
                LOG.warn("ClickHouse connection could not be closed: {}", e.getMessage());
            } finally {
                this.connection = null;
            }
        }
    }

}
