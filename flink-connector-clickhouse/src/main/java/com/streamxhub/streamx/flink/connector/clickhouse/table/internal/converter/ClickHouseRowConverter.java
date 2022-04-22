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
package com.streamxhub.streamx.flink.connector.clickhouse.table.internal.converter;

import com.streamxhub.streamx.flink.connector.shims.sql.clickhouse.ClickhouseArrayImp;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.util.Preconditions;
import ru.yandex.clickhouse.ClickHouseArray;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

/**
 * @author benjobs
 */
public class ClickHouseRowConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final RowType rowType;

    private final DeserializationConverter[] toFlinkConverters;

    private final SerializationConverter[] toClickHouseConverters;

    public ClickHouseRowConverter(RowType rowType) {
        this.rowType = Preconditions.checkNotNull(rowType);
        LogicalType[] fieldTypes = rowType.getFields().stream().map(RowType.RowField::getType).toArray(LogicalType[]::new);
        this.toFlinkConverters = new DeserializationConverter[rowType.getFieldCount()];
        this.toClickHouseConverters = new SerializationConverter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            this.toFlinkConverters[i] = createToFlinkConverter(rowType.getTypeAt(i));
            this.toClickHouseConverters[i] = createToClickHouseConverter(fieldTypes[i]);
        }
    }

    public RowData toFlink(ResultSet resultSet) throws SQLException {
        GenericRowData genericRowData = new GenericRowData(this.rowType.getFieldCount());
        for (int index = 0; index < genericRowData.getArity(); index++) {
            Object field = resultSet.getObject(index + 1);
            genericRowData.setField(index, this.toFlinkConverters[index].deserialize(field));
        }
        return genericRowData;
    }

    public void toClickHouse(RowData rowData, PreparedStatement statement) throws Exception {
        for (int pos = 0; pos < rowData.getArity(); pos++) {
            this.toClickHouseConverters[pos].serialize(rowData, pos, statement);
        }
    }

    private SerializationConverter createToClickHouseConverter(LogicalType type) {
        return (rowData, pos, statement) -> {
            int index = pos + 1;
            if (rowData == null || rowData.isNullAt(pos) || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                statement.setObject(index, null);
            } else {
                switch (type.getTypeRoot()) {
                    case BOOLEAN:
                        statement.setBoolean(index, rowData.getBoolean(pos));
                        break;
                    case TINYINT:
                        statement.setByte(index, rowData.getByte(pos));
                        break;
                    case SMALLINT:
                        statement.setShort(index, rowData.getShort(pos));
                        break;
                    case INTERVAL_YEAR_MONTH:
                    case INTEGER:
                        statement.setInt(index, rowData.getInt(pos));
                        break;
                    case INTERVAL_DAY_TIME:
                    case BIGINT:
                        statement.setLong(index, rowData.getLong(pos));
                        break;
                    case FLOAT:
                        statement.setFloat(index, rowData.getFloat(pos));
                        break;
                    case CHAR:
                    case VARCHAR:
                        statement.setString(index, rowData.getString(pos).toString());
                        break;
                    case VARBINARY:
                        statement.setBytes(index, rowData.getBinary(pos));
                        break;
                    case DATE:
                        statement.setDate(index, Date.valueOf(LocalDate.ofEpochDay(rowData.getInt(pos))));
                        break;
                    case TIME_WITHOUT_TIME_ZONE:
                        statement.setTime(index, Time.valueOf(LocalTime.ofNanoOfDay(rowData.getInt(pos) * 1000000L)));
                        break;
                    case TIMESTAMP_WITH_TIME_ZONE:
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                        int timestampPrecision = ((TimestampType) type).getPrecision();
                        statement.setTimestamp(index, rowData.getTimestamp(pos, timestampPrecision).toTimestamp());
                        break;
                    case DECIMAL:
                        int decimalPrecision = ((DecimalType) type).getPrecision();
                        int decimalScale = ((DecimalType) type).getScale();
                        statement.setBigDecimal(
                                index,
                                rowData.getDecimal(pos, decimalPrecision, decimalScale).toBigDecimal()
                        );
                        break;
                    case ARRAY:
                        ArrayData arrayData = rowData.getArray(pos);
                        ClickHouseArray array = toClickHouseArray(arrayData, type);
                        statement.setArray(index, array);
                        break;
                    case MAP:
                        MapData mapData = rowData.getMap(pos);
                        if (mapData != null && mapData.size() > 0) {
                            MapType mapType = (MapType) type;
                            LogicalType keyType = mapType.getKeyType();
                            LogicalType valueType = mapType.getValueType();
                            if (mapData instanceof BinaryMapData) {
                                BinaryMapData binaryMapData = (BinaryMapData) mapData;
                                BinaryArrayData keys = binaryMapData.keyArray();
                                BinaryArrayData values = binaryMapData.valueArray();
                                Object[] keyArray = keys.toObjectArray(keyType);
                                Object[] valueArray = values.toObjectArray(valueType);
                                Map<String, Object> map = new HashMap<>();
                                for (int i = 0; i < keyArray.length; i++) {
                                    Object value = valueArray[i];
                                    //TODO: value可能是一个复杂类型的对象,需要处理...
                                    map.put(keyArray[i].toString(), value.toString());
                                }
                                statement.setObject(index, map);
                            } else {
                                statement.setObject(index, null);
                            }
                        } else {
                            statement.setObject(index, null);
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported type:" + type);
                }
            }
        };
    }

    private ClickHouseArray toClickHouseArray(ArrayData arrayData, LogicalType type) {
        return new ClickhouseArrayImp().toClickHouseArray(arrayData, type);
    }

    private ArrayData toFlinkArray(ClickHouseArray chArray, LogicalType type) throws SQLException {
        ArrayType arrayType = (ArrayType) type;
        final Class<?> elementClass = LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
        Object[] in = (Object[]) chArray.getArray();
        final Object[] array = (Object[]) java.lang.reflect.Array.newInstance(elementClass, in.length);
        for (int i = 0; i < in.length; i++) {
            array[i] = createToFlinkConverter(arrayType.getElementType()).deserialize(in[i]);
        }
        return new GenericArrayData(array);
    }

    private DeserializationConverter createToFlinkConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case INTEGER:
            case BIGINT:
                return val -> val;
            case TINYINT:
                return val -> ((Integer) val).byteValue();
            case SMALLINT:
                return val -> (val instanceof Integer)
                        ? Short.valueOf(((Integer) val).shortValue())
                        : val;
            case DECIMAL:
                int precision = ((DecimalType) type).getPrecision();
                int scale = ((DecimalType) type).getScale();
                return val -> (val instanceof BigInteger)
                        ? DecimalData.fromBigDecimal(new BigDecimal((BigInteger) val, 0), precision, scale)
                        : DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
            case DATE:
                return val -> (int) ((Date) val).toLocalDate().toEpochDay();
            case TIME_WITHOUT_TIME_ZONE:
                return val -> (int) (((Time) val).toLocalTime().toNanoOfDay() / 1000000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> TimestampData.fromTimestamp((Timestamp) val);
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString((String) val);
            case BINARY:
            case VARBINARY:
                return val -> val;
            case ARRAY:
                return val -> toFlinkArray((ClickHouseArray) val, type);
            case MAP:
                return val -> val;
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private Object toFlinkMap(Object val, LogicalType type) {
        return null;
    }

    @FunctionalInterface
    interface SerializationConverter extends Serializable {
        /**
         * @param rowData
         * @param pos
         * @param preparedStatement
         * @throws Exception
         */
        void serialize(RowData rowData, int pos, PreparedStatement preparedStatement) throws Exception;
    }


    @FunctionalInterface
    interface DeserializationConverter extends Serializable {
        /**
         * deserialize
         *
         * @param paramObject
         * @return
         * @throws SQLException
         */
        Object deserialize(Object paramObject) throws SQLException;
    }
}
