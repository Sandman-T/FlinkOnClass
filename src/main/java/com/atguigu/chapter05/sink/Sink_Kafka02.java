package com.atguigu.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @description:
 * @create_time: 14:14 2021/8/12
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Sink_Kafka02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("sensor_1", 1547718199L, 35),
                new WaterSensor("sensor_6", 1547718201L, 15),
                new WaterSensor("sensor_7", 1547718202L, 6),
                new WaterSensor("sensor_10", 1547718205L, 38)
        );

        source.map(JSON::toJSONString)
                .addSink(new FlinkKafkaProducer<String>(
                "hadoop102:9092",
                "sensor_sink",
                new SimpleStringSchema()
        ));

        env.execute();
    }
}
