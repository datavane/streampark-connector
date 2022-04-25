package com.streamxhub.streamx.flink.connector.shims.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;

/**
 * @author Whojohn
 */
public interface RowJsonTranslate {

    void iniDeser(RowType rowType, LogicalType logicalType);


    void iniSeri(RowType rowType);

    /**
     * @param message
     * @return
     * @throws Exception
     */
    RowData deserialize(byte[] message) throws Exception;

    byte[] serialize(RowData row);

}
