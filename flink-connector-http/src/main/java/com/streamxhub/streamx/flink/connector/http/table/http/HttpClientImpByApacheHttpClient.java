package com.streamxhub.streamx.flink.connector.http.table.http;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.streamxhub.streamx.flink.connector.http.table.HttpDynamicFactory.REQUEST_RETRY_INTERVAL;
import static com.streamxhub.streamx.flink.connector.http.table.HttpDynamicFactory.REQUEST_RETRY_MAX;
import static com.streamxhub.streamx.flink.connector.http.table.HttpDynamicFactory.REQUEST_THREAD;
import static com.streamxhub.streamx.flink.connector.http.table.HttpDynamicFactory.REQUEST_TIMEOUT;

/**
 * @author Whojohn
 */
public class HttpClientImpByApacheHttpClient implements HttpClient<CloseableHttpAsyncClient, SimpleHttpRequest, SimpleHttpRequest>, Serializable {

    private transient PoolingAsyncClientConnectionManager poolManager;
    private transient CloseableHttpAsyncClient httpclient;


    @Override
    public CloseableHttpAsyncClient ini(ReadableConfig options) {
        IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                .setIoThreadCount(options.get(REQUEST_THREAD))
                .setSoTimeout(Timeout.ofMilliseconds(options.get(REQUEST_TIMEOUT)))
                .build();

        // Connector Pool setting
        //设置连接池大小
        poolManager = PoolingAsyncClientConnectionManagerBuilder.create()
                .setMaxConnPerRoute(options.get(REQUEST_THREAD))
                .setMaxConnTotal(options.get(REQUEST_THREAD))
                .setValidateAfterInactivity(TimeValue.ofMilliseconds(options.get(REQUEST_TIMEOUT)))
                .build();

        this.httpclient = HttpAsyncClients.custom()
                .setConnectionManager(poolManager)
                .setRetryStrategy(
                        new DefaultHttpRequestRetryStrategy(options.get(REQUEST_RETRY_MAX),
                                TimeValue.ofMilliseconds(options.get(REQUEST_RETRY_INTERVAL)))
                )
                .setIOReactorConfig(ioReactorConfig)
                .evictIdleConnections(TimeValue.ofMilliseconds(options.get(REQUEST_TIMEOUT)))
                .evictExpiredConnections()
                .build();
        httpclient.start();
        return this.httpclient;
    }

    @Override
    public CloseableHttpAsyncClient getClient() {
        return this.httpclient;
    }

    @Override
    public SimpleHttpRequest get(String url, Map<String, String> header, Map<String, String> queryPara) throws URISyntaxException {
        URIBuilder uriBuilder = new URIBuilder(url);
        for (Map.Entry<String, String> each : queryPara.entrySet()) {
            uriBuilder.addParameter(each.getKey(), each.getValue());
        }
        URI uri = uriBuilder.build();
        SimpleRequestBuilder request = SimpleRequestBuilder.get(uri);
        for (Map.Entry<String, String> each : header.entrySet()) {
            request.addHeader(each.getKey(), each.getValue());
        }
        return request.build();
    }

    @Override
    public SimpleHttpRequest post(String url, Map<String, String> header, byte[] queryJson) {
        SimpleRequestBuilder request = SimpleRequestBuilder.post(url)
                .setBody(queryJson, ContentType.APPLICATION_JSON);
        for (Map.Entry<String, String> each : header.entrySet()) {
            request.addHeader(each.getKey(), each.getValue());
        }
        return request.build();
    }


    @Override
    public void execute(SimpleHttpRequest postRequest, SimpleHttpRequest getRequest, CompletableFuture<byte[]> future) {
        this.httpclient.execute(getRequest == null ? postRequest : getRequest, new FutureCallback<SimpleHttpResponse>() {
            @Override
            public void completed(SimpleHttpResponse simpleHttpResponse) {
                try {
                    // http 2xx 才进行解析 ，否则返回 null
                    if (simpleHttpResponse.getCode() < 300) {
                        future.complete(simpleHttpResponse.getBodyText().getBytes());
                    } else {
                        future.complete(new byte[0]);
                    }
                } catch (Exception e) {

                    future.complete(new byte[0]);
                }
            }

            @Override
            public void failed(Exception ex) {
                // make sure data output
                // 保证数据正常输出
                future.complete(new byte[0]);
            }

            @Override
            public void cancelled() {
                // make sure data output
                // 保证数据正常输出
                future.complete(new byte[0]);
            }
        });

    }

    @Override
    public CloseableHttpAsyncClient close() throws IOException {
        this.httpclient.close();
        return this.httpclient;
    }
}
