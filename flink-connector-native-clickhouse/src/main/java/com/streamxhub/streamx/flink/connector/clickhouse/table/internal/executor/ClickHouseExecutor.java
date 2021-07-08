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

import com.github.housepower.jdbc.ClickHouseConnection;
import com.streamxhub.streamx.flink.connector.clickhouse.table.internal.ClickHouseStatementFactory;
import com.streamxhub.streamx.flink.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.streamxhub.streamx.flink.connector.clickhouse.table.internal.converter.ClickHouseRowConverter;
import com.streamxhub.streamx.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

/**
 * @author benjobs
 */
public interface ClickHouseExecutor extends Serializable {

    void prepareStatement(Connection clickHouseConnection) throws SQLException;

    void prepareStatement(ClickHouseConnectionProvider clickHouseConnectionProvider) throws SQLException;

    void setRuntimeContext(RuntimeContext context);

    void addBatch(RowData rowData) throws IOException;

    void executeBatch() throws IOException;

    void closeStatement() throws SQLException;

    String getState();

    static ClickHouseUpsertExecutor createUpsertExecutor(String tableName,
                                                         String[] fieldNames,
                                                         String[] keyFields,
                                                         ClickHouseRowConverter converter,
                                                         ClickHouseOptions options) {
        String insertSql = ClickHouseStatementFactory.getInsertIntoStatement(tableName, fieldNames);
        String updateSql = ClickHouseStatementFactory.getUpdateStatement(tableName, fieldNames, keyFields,
                Optional.empty());
        String deleteSql = ClickHouseStatementFactory.getDeleteStatement(tableName, keyFields, Optional.empty());
        return new ClickHouseUpsertExecutor(insertSql, updateSql, deleteSql, converter, options);
    }
}
