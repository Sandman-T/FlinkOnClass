package com.atguigu.preview.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @description:
 * @create_time: 23:01 2021/8/5
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink03_Sink_ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));


        HttpHost hadoop102 = new HttpHost("hadoop102", 9200);
        HttpHost hadoop103 = new HttpHost("hadoop103", 9200);
        HttpHost hadoop104 = new HttpHost("hadoop104", 9200);

        List<HttpHost> hostList = Arrays.asList(hadoop102, hadoop103, hadoop104);

        env.fromCollection(waterSensors)
                .addSink(new ElasticsearchSink.Builder<WaterSensor>(hostList, new MyES()).build());

        env.execute();
    }

    public static class MyES implements ElasticsearchSinkFunction<WaterSensor> {
        @Override
        public void open() throws Exception {
            ElasticsearchSinkFunction.super.open();
        }

        @Override
        public void close() throws Exception {
            ElasticsearchSinkFunction.super.close();
        }

        @Override
        public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {
            // 1. 创建es写入请求
            IndexRequest request = Requests
                    .indexRequest("sensor")
                    .type("_doc")
                    .id(element.getId())
                    .source(JSON.toJSONString(element), XContentType.JSON);
            // 2. 写入到es
            indexer.add(request);

        }
    }
}
