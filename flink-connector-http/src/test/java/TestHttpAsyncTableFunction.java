import com.streamxhub.streamx.flink.connector.http.table.HttpAsyncTableFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.junit.Ignore;
import org.junit.Test;
import scala.tools.jline_embedded.internal.Log;

import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

public class TestHttpAsyncTableFunction {

    /**
     * 测试 Http 逻辑(无需启动 Flink 框架)
     *
     * @throws Exception
     */
    @Test
    @Ignore
    public void GetPassTestCase() throws Exception {
        long st = System.currentTimeMillis();

        HttpAsyncTableFunction lookupFunction = new HttpAsyncTableFunction(Configuration.fromMap(new HashMap<String, String>() {{
            put("request.mothod", "get");
            put("url", "http://127.0.0.1:80/sql/hints");
            put("request.thread", "1");
            put("request.max.interval", "10000");
            put("request.limit.gap", "9000");
            put("request.limit.sleep.ms", "3000");
            put("request.retry.max", "3");
            put("request.timeout", "3000");

        }}),
                new String[]{"following_count", "row_t"},
                new DataType[]{DataTypes.BIGINT(), DataTypes.ROW(DataTypes.FIELD("i", DataTypes.INT()), DataTypes.FIELD("s2", DataTypes.STRING()))},
                new String[]{"row_t"},
                200000,
                false
        );
        lookupFunction.open(null);
        for (int a = 0; a < 40000; a++) {
            CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
            GenericRowData te = new GenericRowData(2);
            te.setField(0, 1);
            // notice RowData not support java String
            te.setField(1, StringData.fromString("test string"));


            lookupFunction.eval(future, te);
            future.whenComplete(
                    (rs, t) -> {
                    });
        }
        lookupFunction.close();
        Log.info("Finish total cost(ms):", System.currentTimeMillis() - st);
    }
}
