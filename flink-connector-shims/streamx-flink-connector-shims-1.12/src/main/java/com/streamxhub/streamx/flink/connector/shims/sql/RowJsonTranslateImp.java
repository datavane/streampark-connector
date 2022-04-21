package com.streamxhub.streamx.flink.connector.shims.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author Whojohn
 */
public class RowJsonTranslateImp implements RowJsonTranslate, Serializable {

    private JsonRowDataDeserializationSchema deserializationSchema;
    private JsonRowDataSerializationSchema serializationSchema;

    public void iniDeser(RowType rowType, TypeInformation<RowData> resultTypeInfo) {
        this.deserializationSchema =
                new JsonRowDataDeserializationSchema(rowType, resultTypeInfo, false, true, TimestampFormat.ISO_8601);

    }

    public void iniSeri(RowType rowType) {
        this.serializationSchema =
                new JsonRowDataSerializationSchema(rowType,
                        TimestampFormat.ISO_8601,
                        JsonOptions.MapNullKeyMode.LITERAL,
                        "null");
    }


    public RowData deserialize(byte[] message) throws IOException {
        return deserializationSchema.deserialize(message);
    }

    public byte[] serialize(RowData row) {
        return serializationSchema.serialize(row);
    }
}
