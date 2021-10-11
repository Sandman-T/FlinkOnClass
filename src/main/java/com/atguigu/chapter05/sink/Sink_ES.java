package com.atguigu.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Arrays;
import java.util.List;

/**
 * @description:
 * @create_time: 20:58 2021/8/9
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Sink_ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9527);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = source.map(line -> {
            String[] strings = line.split(" ");
            return new WaterSensor(strings[0], Long.parseLong(strings[1]), Integer.parseInt(strings[2]));
        });

        List<HttpHost> httpHosts = Arrays.asList(
                new HttpHost("hadoop102", 9200),
                new HttpHost("hadoop103", 9200),
                new HttpHost("hadoop104", 9200)
        );

        ElasticsearchSink.Builder<WaterSensor> waterSensorBuilder = new ElasticsearchSink.Builder<>(httpHosts,
                new ElasticsearchSinkFunction<WaterSensor>() {
                    @Override
                    public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {
                        String jsonString = JSON.toJSONString(element);

                        IndexRequest indexRequest = new IndexRequest()
                                .index("sensor")
                                .type("_doc")
                                .id("1001")
                                .source(jsonString, XContentType.JSON); // json数据需要设置类型，否则ES无法识别

                        // 这种方式也可以
                        IndexRequest indexRequest1 = Requests.indexRequest()
                                .index("sensor")
                                .type("_doc")
                                .id("1001")
                                .source(jsonString, XContentType.JSON);

                        // 也可以直接写一个java bean
                        IndexRequest indexRequest2 = Requests.indexRequest()
                                .index("sensor")
                                .type("_doc")
                                .id("1001")
                                .source(element);

                        indexer.add(indexRequest);
                    }
                }
        );

        waterSensorBuilder.setBulkFlushMaxActions(2);

        waterSensorDS.addSink(waterSensorBuilder.build());

        env.execute();
    }
}
