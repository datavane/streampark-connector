package com.streamxhub.streamx.flink.connector.shims.sql.clickhouse;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.types.logical.LogicalType;
import ru.yandex.clickhouse.ClickHouseArray;

public interface ClickhouseArray {
    ClickHouseArray toClickHouseArray(ArrayData arrayData, LogicalType type);
}
