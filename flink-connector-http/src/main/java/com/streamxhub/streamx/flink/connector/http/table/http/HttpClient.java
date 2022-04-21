package com.streamxhub.streamx.flink.connector.http.table.http;

import org.apache.flink.configuration.ReadableConfig;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * @author Whojohn
 * C is Http client instance class , G is http get request instance class , P is http post request instance class
 */
public interface HttpClient<C, G, P> {
    /**
     * 初始化 http client 实现
     * ini http client
     *
     * @return C
     */
    C ini(ReadableConfig options) throws Exception;

    C getClient();

    /**
     * Build up http get request
     * 将 queryPara 组装为 http get 请求
     *
     * @param url       request url
     * @param header    http header
     * @param queryPara query params such as : Map<String, String>
     * @return G http get 请求的返回返回 json
     */
    G get(String url, Map<String, String> header, Map<String, String> queryPara) throws Exception;

    /**
     * Build up http post request which only request in application/json.
     * 将 queryJson 组装为 http post json 请求
     *
     * @param url       request url
     * @param header    http header
     * @param queryJson query json serialize in byte[]
     * @return P return build success Post request
     */
    P post(String url, Map<String, String> header, byte[] queryJson) throws Exception;

    /**
     * Send request by post or get method .Only support one method in per request. Any error return byte[0]
     * 发送 http get , post 请求中的一者(不能同时存在 post get 请求)。异常返回 byte [0]
     *
     * @param postRequest null or postRequest
     * @param getRequest  null or getRequest
     * @param future      CompletableFuture<byte[]>.complete to support async http
     */
    void execute(P postRequest, G getRequest, CompletableFuture<byte[]> future);

    /**
     * elegant exit http client
     * 优雅退出 client 实现
     *
     * @return return client instance
     * @throws Exception throw any exception
     */
    C close() throws Exception;
}
