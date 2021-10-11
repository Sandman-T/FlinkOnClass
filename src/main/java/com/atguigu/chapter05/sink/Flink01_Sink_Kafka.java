package com.atguigu.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Arrays;
import java.util.List;

/**
 * @description:
 * @create_time: 9:15 2021/8/8
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink01_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("sensor_1", 1547718199L, 35),
                new WaterSensor("sensor_6", 1547718201L, 15),
                new WaterSensor("sensor_7", 1547718202L, 6),
                new WaterSensor("sensor_10", 1547718205L, 38)
        );

        env.fromCollection(waterSensors)
                .map(JSON::toJSONString)
                .addSink(new FlinkKafkaProducer<String>(
                        "hadoop102:9092",
                        "sensor_sink",
                        new SimpleStringSchema()
                ));

        env.execute();
    }
}
