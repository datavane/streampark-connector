package com.streamxhub.streamx.flink.connector.http.table.metric;

import com.streamxhub.streamx.flink.connector.http.table.HttpDynamicFactory;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;

import java.io.Serializable;

import static com.streamxhub.streamx.flink.connector.http.table.HttpDynamicFactory.URL;

/**
 * @author Whojohn
 */
public class HttpMetric implements Serializable {
    private final transient MetricGroup baseGroup;
    private final transient Counter successCount;
    private final transient Counter failCount;
    private final transient Counter deserializeFail;
    private final transient Counter unknownFail;
    private final transient Counter httpFail;
    private final boolean metric;


    public HttpMetric(FunctionContext context, ReadableConfig options, boolean metric) {
        this.metric = metric;
        this.baseGroup = context.getMetricGroup()
                .addGroup("HTTP")
                .addGroup("Async")
                .addGroup(options.get(HttpDynamicFactory.REQUEST_METHOD))
                .addGroup(options.get(URL));

        this.successCount = this.baseGroup.counter("success");
        // fail = deserializeFail + unknownFail + httpFail
        this.failCount = this.baseGroup.counter("fail");
        this.deserializeFail = this.baseGroup.counter("deserializeFail");
        this.unknownFail = this.baseGroup.counter("unknownFail");
        this.httpFail = this.baseGroup.counter("httpFail");
    }

    public void success() {
        if (this.metric) {
            this.successCount.inc();
        }
    }

    public void fail() {
        if (this.metric) {
            this.failCount.inc();
        }
    }

    public void deserializeFail() {
        if (this.metric) {
            this.deserializeFail.inc();
        }
    }

    public void unknownFail() {
        if (this.metric) {
            this.unknownFail.inc();
        }
    }

    public void httpFail() {
        if (this.metric) {
            this.httpFail.inc();
        }
    }
}
