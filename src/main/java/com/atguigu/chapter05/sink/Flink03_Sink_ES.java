package com.atguigu.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * @description:
 * @create_time: 9:45 2021/8/8
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

        env.fromCollection(waterSensors)
                .addSink(new ElasticsearchSink.Builder<>(
                        Collections.singletonList(new HttpHost("hadoop102", 9200)), new MyES()
                        ).build()
                );


        env.execute();
    }

    public static class MyES implements ElasticsearchSinkFunction<WaterSensor> {

        @Override
        public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {

            IndexRequest request = Requests.indexRequest()
                    .index("water_sensor")
                    .type("_doc")
                    .id(element.getId())
                    .source(JSON.toJSONString(element));

            indexer.add(request);

        }
    }
}
