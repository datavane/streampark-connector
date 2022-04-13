/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamxhub.streamx.flink.connector.pulsar.table;

import com.alibaba.fastjson.JSONObject;
import com.streamxhub.streamx.flink.connector.pulsar.table.util.PulsarConnectionHolder;
import com.streamxhub.streamx.flink.connector.pulsar.table.util.PulsarProducerHolder;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;


/**
 * The sink function for Pulsar.
  @author DarrenDa
   * @version 1.0
 * @Desc:
 */
@Internal
public class PulsarSinkFunction_bak<T> extends RichSinkFunction<T>
        implements CheckpointedFunction{

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PulsarSinkFunction_bak.class);

    private final String topic;
    private final String serviceUrl;
    private final Properties pulsarProducerProperties;
    private final Properties pulsarClientProperties;
    SerializationSchema<T> runtimeEncoder;
//    private transient PulsarClient pulsarClient;
    private transient Producer producer;
    private transient volatile boolean closed = false;


    public PulsarSinkFunction_bak(
            String topic,
            String serviceUrl,
            Properties pulsarProducerProperties,
            Properties pulsarClientProperties,
            SerializationSchema<T> runtimeEncoder
            ) {
        this.topic = topic;
        this.serviceUrl=serviceUrl;
        this.pulsarProducerProperties = pulsarProducerProperties;
        this.pulsarClientProperties = pulsarClientProperties;
        this.runtimeEncoder = runtimeEncoder;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("start open ...");
        try {
            RuntimeContext ctx = getRuntimeContext();

            LOG.info("Starting FlinkPulsarProducer ({}/{}) to produce into (※) pulsar topic {}",
                    ctx.getIndexOfThisSubtask() + 1, ctx.getNumberOfParallelSubtasks(), topic);

//            this.producer = createProducer();
            this.producer = createReusedProducer();
            LOG.info("Pulsar producer has been created.");

        } catch (IOException ioe) {
            LOG.error("Exception while creating connection to Pulsar.", ioe);
            throw new RuntimeException("Cannot create connection to Pulsar.", ioe);
        }catch (Exception ex){
            LOG.error("Exception while creating connection to Pulsar.", ex);
            throw new RuntimeException("Cannot create connection to Pulsar.", ex);
        }
        LOG.info("end open.");
    }


    @Override
    public void invoke(T value, Context context) throws Exception {
        LOG.info("start to invoke, send pular message.");

        byte[] serializeValue = runtimeEncoder.serialize(value);
        String strValue = new String(serializeValue);
        TypedMessageBuilder<byte[]> typedMessageBuilder = producer.newMessage();
        typedMessageBuilder.value(serializeValue);
        typedMessageBuilder.key(getKey(strValue));
        typedMessageBuilder.send();
    }


    @Override
    public void close() throws Exception {

        //采用pulsar producer复用的方式，close方法不要具体实现，否则producer会被关闭
        LOG.error("PulsarProducerBase Class close function called");
//        closed = true;
//
//        if (producer != null) {
//            try {
//                producer.close();
//            } catch (IOException e) {
//                LOG.warn("Exception occurs while closing Pulsar producer.", e);
//            }
//            this.producer = null;
//        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        //
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // nothing to do.
    }


    public String getKey(String strValue){
        JSONObject jsonObject = JSONObject.parseObject(strValue);
        String key = jsonObject.getString("key");
        return key == null ? "" : key;
    }

    //获取Pulsar Producer
    public Producer createProducer() throws Exception{
        LOG.info("current pulsar version is " + PulsarVersion.getVersion());

        ClientBuilder builder = PulsarClient.builder();
        ProducerBuilder producerBuilder = builder.serviceUrl(serviceUrl)
                .maxNumberOfRejectedRequestPerConnection(50)
                .loadConf((Map)pulsarClientProperties)
                .build()
                .newProducer()
                .topic(topic)
                .blockIfQueueFull(Boolean.TRUE)
                .compressionType(CompressionType.LZ4)
                .hashingScheme(HashingScheme.JavaStringHash)
//                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                .loadConf((Map) pulsarProducerProperties);//实现配置透传功能
        Producer producer = producerBuilder.create();
        return producer;

//        return PulsarClient.builder()
//                .serviceUrl(serviceUrl)
//                .build()
//                .newProducer()
//                .loadConf((Map)properties)//实现配置透传功能
//                .topic(topic)
//                .blockIfQueueFull(Boolean.TRUE)
//                .compressionType(CompressionType.LZ4)
//                .hashingScheme(HashingScheme.JavaStringHash)
//                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
//                .create();
    }

    //获取复用的Pulsar Producer
    public Producer createReusedProducer() throws Exception{
        LOG.info("now create client, serviceUrl is :" + serviceUrl);
        PulsarClientImpl client = PulsarConnectionHolder.getProducerClient(serviceUrl, pulsarClientProperties);

        LOG.info("current pulsar version is " + PulsarVersion.getVersion());

        LOG.info("now create producer, topic is :" + topic);
//        ProducerConfigurationData configuration = new ProducerConfigurationData();
//        configuration.setHashingScheme(HashingScheme.JavaStringHash);
        return PulsarProducerHolder.getProducer(topic, pulsarProducerProperties, client);

    }

}
