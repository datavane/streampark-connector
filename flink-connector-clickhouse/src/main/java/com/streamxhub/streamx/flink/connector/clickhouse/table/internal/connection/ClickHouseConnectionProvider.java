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

import com.streamxhub.streamx.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.http.client.utils.URIBuilder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author benjobs
 */
public class ClickHouseConnectionProvider implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Pattern PATTERN = Pattern.compile("You must use port (?<port>[0-9]+) for HTTP.");

    //jdbc url模板

    private static final Pattern URL_TEMPLATE = Pattern.compile("clickhouse:" +
            "//([a-zA-Z0-9_:,.-]+)" +
            "((/[a-zA-Z0-9_]+)?" +
            "([?][a-zA-Z0-9_]+[=][a-zA-Z0-9_]+([&][a-zA-Z0-9_]+[=][a-zA-Z0-9_]*)*)?" +
            ")?");

    private final ClickHouseOptions options;

    private final Map<String, HikariDataSource> dataSourceHolder = new ConcurrentHashMap<>();

    private final Map<String, Connection> shardConnections = new ConcurrentHashMap<>();

    public ClickHouseConnectionProvider(ClickHouseOptions options) {
        this.options = options;
    }

    public Connection getConnection() throws SQLException {
        String jdbcUrl = this.getJdbcUrl(this.options.getUrl(), this.options.getDatabaseName());
        if (this.dataSourceHolder.isEmpty() && !this.dataSourceHolder.containsKey(this.options.getUrl())) {
            synchronized (ClickHouseConnectionProvider.class) {
                if (this.dataSourceHolder.isEmpty() && !this.dataSourceHolder.containsKey(this.options.getUrl())) {
                    createDataSource(jdbcUrl);
                }
            }
        }
        return this.dataSourceHolder.get(jdbcUrl).getConnection();
    }

    /**
     * @param jdbcUrl
     */
    private void createDataSource(String jdbcUrl) {
        HikariConfig conf = new HikariConfig();
        conf.setJdbcUrl(jdbcUrl);
        conf.setDriverClassName("ru.yandex.clickhouse.ClickHouseDriver");
        if (options.getUsername().isPresent()) {
            conf.setUsername(options.getUsername().orElse(null));
            conf.setPassword(options.getPassword().orElse(null));
        }
        HikariDataSource dataSource = new HikariDataSource(conf);
        dataSourceHolder.put(jdbcUrl, dataSource);
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
    public synchronized Collection<Connection> getShardConnections(String remoteCluster, String remoteDataBase) throws SQLException {
        if (this.dataSourceHolder.isEmpty()) {
            Connection conn = getConnection();
            String shardSql = String.format("SELECT shard_num, host_address, port FROM system.clusters WHERE cluster = '%s'", remoteCluster);
            //查询ck集群各个分片信息
            PreparedStatement stmt = conn.prepareStatement(shardSql);
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    String hostAddress = resultSet.getString("host_address");
                    int port = getActualHttpPort(hostAddress, resultSet.getInt("port"));
                    String url = String.format("clickhouse://%s:%d", hostAddress, port);
                    String jdbcUrl = getJdbcUrl(url, remoteDataBase);
                    createDataSource(jdbcUrl);
                    Connection connection = dataSourceHolder.get(jdbcUrl).getConnection();
                    this.shardConnections.put(jdbcUrl, connection);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (this.shardConnections.isEmpty()) {
                throw new SQLException("unable to query shards in system.clusters");
            }
        }
        return this.shardConnections.values();
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

    private String getJdbcUrl(String url, String dbName) throws SQLException {
        try {
            return "jdbc:" + (new URIBuilder(url)).setPath("/" + dbName).build().toString();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public void closeConnection() throws SQLException {
        HikariDataSource dataSource = dataSourceHolder.get(getJdbcUrl(options.getUrl(), options.getDatabaseName()));
        if (dataSource != null) {
            dataSource.close();
        }
        if (!this.shardConnections.isEmpty()) {
            for (String url : shardConnections.keySet()) {
                dataSourceHolder.get(url).close();
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
