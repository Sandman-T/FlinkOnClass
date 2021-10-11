package com.atguigu.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Arrays;
import java.util.List;

/**
 * @description:
 * @create_time: 14:22 2021/8/12
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Sink_ES02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("sensor_1", 1547718199L, 35),
                new WaterSensor("sensor_6", 1547718201L, 15),
                new WaterSensor("sensor_7", 1547718202L, 6),
                new WaterSensor("sensor_10", 1547718205L, 38)
        );

        List<HttpHost> httpHosts = Arrays.asList(
                new HttpHost("hadoop102", 9200),
                new HttpHost("hadoop103", 9200),
                new HttpHost("hadoop104", 9200)
        );

        ElasticsearchSink.Builder<WaterSensor> esSinkBuilder = new ElasticsearchSink.Builder<>(httpHosts,
                new ElasticsearchSinkFunction<WaterSensor>() {
                    @Override
                    public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {
                        IndexRequest indexRequest = Requests.indexRequest()
                                .index("sensor")
                                .type("_doc")
                                .id("10086")
                                .source(JSON.toJSONString(element), XContentType.JSON);

                        indexer.add(indexRequest);
                    }
                });

        // 设置几条数据触发一次写入
        esSinkBuilder.setBulkFlushMaxActions(2);

        source.addSink(esSinkBuilder.build());

        env.execute();
    }
}
