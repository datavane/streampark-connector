package com.streamxhub.streamx.flink.connector.http.table;

import com.streamxhub.streamx.flink.connector.http.table.http.HttpClient;
import com.streamxhub.streamx.flink.connector.http.table.http.HttpClientImpByApacheHttpClient;
import com.streamxhub.streamx.flink.connector.http.table.metric.HttpMetric;
import com.streamxhub.streamx.flink.connector.shims.sql.RowJsonTranslate;
import com.streamxhub.streamx.flink.connector.shims.sql.RowJsonTranslateImp;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static com.streamxhub.streamx.flink.connector.http.table.HttpDynamicFactory.REQUEST_METHOD;
import static com.streamxhub.streamx.flink.connector.http.table.HttpDynamicFactory.URL;
import static org.apache.flink.table.api.DataTypes.ROW;


/**
 * @author Whojohn
 */

public class HttpAsyncTableFunction extends AsyncTableFunction<RowData> implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(HttpAsyncTableFunction.class.getName());

    private transient AtomicLong count;

    private final HttpClient<CloseableHttpAsyncClient, SimpleHttpRequest, SimpleHttpRequest> httpclient;


    /**
     * For request queue limit
     * sleepMs : Sleep while request more than gap
     * 请求限速配置
     * sleepMs : 达到堆积量睡眠时间
     */
    private Long sleepMs;
    /**
     * For request queue limit
     * gap : Max request in queue . Notice: not strict limit .
     * 请求限速配置
     * gap : 最大请求堆积量(等待完成回调的请求量) . 注意：非严格限制
     */
    private final Integer gap;


    private final ReadableConfig options;
    private final String[] keyNames;
    /**
     * 输出列列名与列类型
     */
    private final Map<String, DataType> typeBySeqMap;
    /**
     * 输出列列名与 rowdata 位置信息
     */
    private final Map<String, Integer> dimLoc;
    /**
     * 输出列 json to row
     * 将输入 row to json
     */
    private final RowJsonTranslate rowJsonTranslate;

    private final long checkpointSec;

    /**
     * metric or not
     * 是否开启监控
     */
    private final boolean metricLabel;
    private HttpMetric metric;


    enum HttpMethod {
        /**
         * Http method enum
         * 对应 http 操作类型
         */
        GET("get"), POST("post");
        private final String name;

        HttpMethod(String name) {
            this.name = name;
        }
    }

    public HttpAsyncTableFunction(ReadableConfig options, String[] fieldNames, DataType[] fieldDataTypes, String[] keyNames, long checkpointSec, boolean metricLabel) {
        this.options = options;
        this.keyNames = keyNames;

        this.sleepMs = Long.valueOf(options.get(HttpDynamicFactory.REQUEST_LIMIT_SLEEP_MS));
        this.gap = options.get(HttpDynamicFactory.REQUEST_LIMIT_GAP);

        this.typeBySeqMap = new LinkedHashMap<>(keyNames.length);
        this.dimLoc = new HashMap<>(fieldNames.length);
        IntStream.range(0, fieldDataTypes.length).forEach(e -> {
            typeBySeqMap.put(fieldNames[e], fieldDataTypes[e]);
            dimLoc.put(fieldNames[e], e);
        });

        this.httpclient = new HttpClientImpByApacheHttpClient();

        // json to row
        // 用于将 http 返回结果 json 解析为输出列格式
        List<DataTypes.Field> schemaList = new ArrayList<>();
        for (String schemaName : this.typeBySeqMap.keySet()) {
            schemaList.add(DataTypes.FIELD(schemaName, this.typeBySeqMap.get(schemaName)));
        }
        DataType dimSchema = ROW(schemaList.toArray(new DataTypes.Field[]{}));

        this.rowJsonTranslate = new RowJsonTranslateImp();
        this.rowJsonTranslate.iniDeser((RowType) dimSchema.getLogicalType(), dimSchema.getLogicalType());

        // row to json
        // 用于 post 请求下 sql to json 请求参数组装
        schemaList.clear();
        Set<String> tarKeyName = new HashSet<>(Arrays.asList(keyNames));
        for (String schemaName : this.typeBySeqMap.keySet()) {
            if (tarKeyName.contains(schemaName)) {
                schemaList.add(DataTypes.FIELD(schemaName, this.typeBySeqMap.get(schemaName)));
            }
        }
        dimSchema = ROW(schemaList.toArray(new DataTypes.Field[]{}));
        this.rowJsonTranslate.iniSeri((RowType) dimSchema.getLogicalType());


        /**
         * Valid logical type in GET request to prevent : row ,map, array type using.
         * 检查类型逻辑在 Get 请求下是否冲突，禁止嵌套类型:row ,map, array
         */
        if (((Configuration) options).getString(HttpDynamicFactory.REQUEST_METHOD).equals(HttpMethod.GET.name)) {
            for (String each : keyNames) {
                if (typeBySeqMap.get(each) instanceof FieldsDataType || typeBySeqMap.get(each) instanceof KeyValueDataType) {
                    throw new IllegalArgumentException("In http get mode not support FieldsDataType nested inside: such as :row ,map, array inside define.");
                }
            }

        }
        this.checkpointSec = checkpointSec;
        this.metricLabel = metricLabel;
    }


    @Override
    public void open(FunctionContext context) throws Exception {
        this.metric = new HttpMetric(context, options, this.metricLabel);
        this.count = new AtomicLong();
        this.httpclient.ini(options);
        super.open(context);
    }

    /**
     * Build up http get params
     * 组装 http Get 请求; ！！！对于单个 row 传入参数会展开 ！！！
     *
     * @param rowKey join 字段
     * @return join 组装为 http 请求参数
     */
    public Map<String, String> buildHTTPGetParams(Object... rowKey) {
        Map<String, String> para = new HashMap<>(rowKey.length);
        int loc = 0;
        for (; loc < keyNames.length; loc++) {
            para.put(this.keyNames[loc], rowKey[loc].toString());
        }
        return para;
    }

    /**
     * 异步维表通过 eval 进行调用
     *
     * @param future The result or exception is returned.
     * @param rowKey the lookup key. Currently  support multi rowkey.
     */
    public void eval(CompletableFuture<Collection<RowData>> future, Object... rowKey) {
        try {

            this.count.addAndGet(1);
            // Request limit logical
            // 限制堆积请求数，防止 oom
            if (this.count.get() > gap) {
                long start = this.count.get();
                if (sleepMs > 1000) {
                    Thread.sleep(sleepMs);
                }
                long after = this.count.get();
                long sleepGap = (long) (start == after ? sleepMs : (sleepMs * 0.1 / (start - after)) * Math.abs(start - gap));
                sleepMs = sleepMs == 0 ? 10 : Math.min(sleepGap, 2 * sleepMs);
            }

            SimpleHttpRequest request = null;
            if (HttpMethod.GET.name.equals(options.get(HttpDynamicFactory.REQUEST_METHOD))) {
                request = this.httpclient.get(options.get(URL), new HashMap<>(0), this.buildHTTPGetParams(rowKey));
            } else if (HttpMethod.POST.name.equals(options.get(HttpDynamicFactory.REQUEST_METHOD))) {
                RowData temp = new GenericRowData(keyNames.length);
                for (int loc = 0; loc < keyNames.length; loc++) {
                    ((GenericRowData) temp).setField(loc, rowKey[loc]);
                }
                request = this.httpclient.post(options.get(URL), new HashMap<>(0), this.rowJsonTranslate.serialize(temp));
            }
            assert request != null;

            CompletableFuture<byte[]> httpFuture = new CompletableFuture<>();
            this.httpclient.execute(options.get(REQUEST_METHOD).equals(HttpMethod.GET.name) ? request : null,
                    options.get(REQUEST_METHOD).equals(HttpMethod.POST.name) ? request : null,
                    httpFuture);
            httpFuture.whenComplete((rs, t) -> {
                try {
                    // 返回 byte[].len ==0 时说明 http 请求失败
                    if (rs.length == 0) {
                        this.metric.httpFail();
                        this.metric.fail();
                        future.complete(Collections.singleton(new GenericRowData(dimLoc.size())));
                    } else {

                        // !!! Notice !!! Potential bug cause by Flink version change !!!
                        // In flink 1.12 as we instance JsonRowDataDeserializationSchema in rowType so that deserializationSchema.deserialize will return GenericRowData.
                        // 注意，Flink 版本升级可能会有潜在 bug 风险
                        // 翻阅源码 1.12 中 JsonRowDataDeserializationSchema 实例化使用了 rowType 只会返回 GenericRowData 类型。
                        GenericRowData httpRes = (GenericRowData) this.rowJsonTranslate.deserialize(rs);
                        // 防止用户不遵守协议返回非 json 数据，或者 json 解析异常
                        // avoid illegal json format or un-satisfaction json format
                        if (httpRes == null) {
                            this.metric.deserializeFail();
                            this.metric.fail();
                            future.complete(Collections.singleton(new GenericRowData(dimLoc.size())));
                            log.warn("Unable to deserialize json as follow:\n" + new String(rs));
                        } else {
                            for (int loc = 0; loc < rowKey.length; loc++) {
                                httpRes.setField(dimLoc.get(keyNames[loc]), rowKey[loc]);
                            }
                            this.metric.success();
                            future.complete(Collections.singleton(httpRes));
                        }
                    }
                } catch (Exception e) {
                    this.metric.unknownFail();
                    this.metric.fail();
                    future.complete(Collections.singleton(new GenericRowData(dimLoc.size())));
                    log.warn(e.getMessage(), e);
                } finally {
                    count.decrementAndGet();
                }

            });

        } catch (Exception e) {
            // make sure data output
            // 保证数据正常输出
            this.metric.unknownFail();
            this.metric.fail();
            log.warn(e.getMessage(), e);
            future.complete(Collections.singleton(new GenericRowData(dimLoc.size())));
        }

    }

    @Override
    public void close() throws Exception {
        long pre = this.count.get();
        int aggCount = 0;
        int sleepMs = 200;

        while (this.count.get() > 0) {
            if (this.count.get() == pre) {
                aggCount += 1;
            } else {
                // avoid deep loop
                // 防止死循环退出逻辑
                if ((aggCount * sleepMs) / 1.0 > this.checkpointSec) {
                    break;
                }
                pre = this.count.get();
                aggCount = 0;
                Thread.sleep(200);
            }
        }

        super.close();
        this.httpclient.close();
    }

}
