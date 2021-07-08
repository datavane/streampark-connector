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
package com.streamxhub.streamx.flink.connector.clickhouse.table.internal.connection;

import com.github.housepower.jdbc.ClickHouseConnection;
import com.github.housepower.jdbc.connect.NativeContext;
import com.streamxhub.streamx.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author benjobs
 */
public class ClickHouseConnectionProvider implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Pattern PATTERN = Pattern.compile("You must use port (?<port>[0-9]+) for HTTP.");

    //jdbc url模板
    private static final Pattern URL_TEMPLATE = Pattern.compile("clickhouse:" + "" +
            "//([a-zA-Z0-9_:,.-]+)" +
            "(/[a-zA-Z0-9_]+" +
            "([?][a-zA-Z0-9_]+[=][a-zA-Z0-9_]+([&][a-zA-Z0-9_]+[=][a-zA-Z0-9_]+)*)?" +
            ")?");

    private transient Connection connection;

    public static transient NativeContext nativeContext;

    private transient List<Connection> shardConnections;

    private static ClickHouseOptions options;

    public ClickHouseConnectionProvider(ClickHouseOptions options) {
        this.options = options;
    }

    public synchronized Connection getConnection() throws SQLException {
        if (connection == null) {
            synchronized (ClickHouseConnectionProvider.class) {
                if (connection == null) {
                    connection = createConnection(options.getUrl(), options.getDatabaseName());
                }
            }
        }
        return connection;
    }


    /**
     * @param url
     * @param dbName
     * @return
     * @throws SQLException
     */
    private Connection createConnection(String url, String dbName) throws SQLException {
        try {
            Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }

        if (options.getUsername().isPresent()) {
            this.connection = DriverManager.getConnection(getJdbcUrl(url, dbName),
                    options.getUsername().orElse(null), options.getPassword().orElse(null));
        } else {
            this.connection = DriverManager.getConnection(getJdbcUrl(url, dbName));
        }
        try {
            Field field = ClickHouseConnection.class.getDeclaredField("nativeCtx");
            field.setAccessible(true);
            nativeContext = (NativeContext) field.get(connection);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    private String parseUrl(String urls) {
        Matcher matcher = URL_TEMPLATE.matcher(urls);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Incorrect url!");
        }
        return "";
    }

    /**
     * 如果采用的是插入单机表模式，分别获取每台机器的jdbc连接
     *
     * @param remoteCluster
     * @param remoteDataBase
     * @return
     * @throws SQLException
     */
    public synchronized List<Connection> getShardConnections(String remoteCluster, String remoteDataBase) throws SQLException {
        if (this.shardConnections == null) {
            Connection conn = getConnection();
            String shardSql = String.format("SELECT shard_num, host_address, port FROM system.clusters WHERE cluster = '%s'", remoteCluster);
            //查询ck集群各个分片信息
            PreparedStatement stmt = conn.prepareStatement(shardSql);

            try (ResultSet resultSet = stmt.executeQuery()) {
                this.shardConnections = new ArrayList<>();
                while (resultSet.next()) {
                    String hostAddress = resultSet.getString("host_address");
                    int port = getActualHttpPort(hostAddress, resultSet.getInt("port"));
                    String url = "clickhouse://" + hostAddress + ":" + port;
                    this.shardConnections.add(createConnection(url, remoteDataBase));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (this.shardConnections.isEmpty()) {
                throw new SQLException("unable to query shards in system.clusters");
            }
        }

        return this.shardConnections;
    }

    private int getActualHttpPort(String hostAddress, int port) throws Exception {
        URL url = new URL(hostAddress + ":" + port);
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setDoOutput(true);
            connection.setUseCaches(false);
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            connection.connect();
            int statusCode = connection.getResponseCode();
            if (statusCode == 200) {
                return port;
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            Scanner scanner = new Scanner(reader);
            StringBuilder resultBuilder = new StringBuilder();
            while (scanner.hasNext()) {
                resultBuilder.append(scanner.next());
                if (scanner.hasNext()) {
                    resultBuilder.append("\n");
                }
            }
            reader.close();
            Matcher matcher = PATTERN.matcher(resultBuilder.toString());
            if (matcher.find()) {
                return Integer.parseInt(matcher.group("port"));
            }
            throw new SQLException("Cannot query ClickHouse http port");
        } catch (Exception e) {
            throw new SQLException(e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static String getJdbcUrl(String url, String dbName) throws SQLException {
        return String.format("jdbc:%s/%s", url, dbName);
    }

    public void closeConnection() throws SQLException {
        if (this.connection != null) {
            this.connection.close();
        }
        if (this.shardConnections != null) {
            for (Connection shardConnection : shardConnections) {
                shardConnection.close();
            }
        }
    }

    /**
     * 根据WITH中传入的distributed表名称获取单机表名称相关信息
     *
     * @param databaseName
     * @param tableName
     * @return
     * @throws SQLException
     */
    public String queryTableEngine(String databaseName, String tableName) throws SQLException {
        Connection conn = getConnection();
        try (PreparedStatement stmt = conn.prepareStatement("SELECT engine_full FROM system.tables WHERE database = ? AND name = ?")) {
            stmt.setString(1, databaseName);
            stmt.setString(2, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("engine_full");
                }
            }
        }
        throw new SQLException("table `" + databaseName + "`.`" + tableName + "` does not exist");
    }
}
