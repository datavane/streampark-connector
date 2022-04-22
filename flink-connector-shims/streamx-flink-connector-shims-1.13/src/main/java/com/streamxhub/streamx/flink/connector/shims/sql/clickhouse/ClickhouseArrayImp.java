package com.streamxhub.streamx.flink.connector.shims.sql.clickhouse;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.ColumnarArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import ru.yandex.clickhouse.ClickHouseArray;
import ru.yandex.clickhouse.domain.ClickHouseDataType;

/**
 * @author Whojohn
 */
public class ClickhouseArrayImp implements ClickhouseArray {
    public ClickHouseArray toClickHouseArray(ArrayData arrayData, LogicalType type) {
        ArrayType arrayType = (ArrayType) type;
        final Class<?> elementClass = LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
        if (arrayData == null) {
            final Object[] array = (Object[]) java.lang.reflect.Array.newInstance(elementClass, 0);
            return new ClickHouseArray(ClickHouseDataType.Array, array);
        }
        if (arrayData instanceof GenericArrayData) {
            GenericArrayData genericArrayData = (GenericArrayData) arrayData;
            Object[] elements = genericArrayData.toObjectArray();
            return new ClickHouseArray(ClickHouseDataType.Array, elements);
        } else if (arrayData instanceof BinaryArrayData) {
            BinaryArrayData binaryArrayData = (BinaryArrayData) arrayData;
            Object[] elements = binaryArrayData.toObjectArray(arrayType.getElementType());
            return new ClickHouseArray(ClickHouseDataType.Array, elements);
        }
        ColumnarArrayData columnarArrayData = (ColumnarArrayData) arrayData;
        if (int[].class.equals(elementClass)) {
            int[] array = columnarArrayData.toIntArray();
            return new ClickHouseArray(ClickHouseDataType.Array, array);
        } else if (long[].class.equals(elementClass)) {
            long[] array = columnarArrayData.toLongArray();
            return new ClickHouseArray(ClickHouseDataType.Array, array);
        } else if (float[].class.equals(elementClass)) {
            float[] array = columnarArrayData.toFloatArray();
            return new ClickHouseArray(ClickHouseDataType.Array, array);
        } else if (double[].class.equals(elementClass)) {
            double[] array = columnarArrayData.toDoubleArray();
            return new ClickHouseArray(ClickHouseDataType.Array, array);
        } else if (short[].class.equals(elementClass)) {
            short[] array = columnarArrayData.toShortArray();
            return new ClickHouseArray(ClickHouseDataType.Array, array);
        } else if (byte[].class.equals(elementClass)) {
            byte[] array = columnarArrayData.toByteArray();
            return new ClickHouseArray(ClickHouseDataType.Array, array);
        } else if (boolean[].class.equals(elementClass)) {
            boolean[] array = columnarArrayData.toBooleanArray();
            return new ClickHouseArray(ClickHouseDataType.Array, array);
        }
        throw new RuntimeException("Unsupported primitive array: ");
    }
}