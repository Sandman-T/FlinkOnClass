package com.atguigu.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;


import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @description:
 * @create_time: 18:08 2021/8/9
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9527);

        SingleOutputStreamOperator<String> jsonStrDS = source.map(line -> {
            String[] strings = line.split(" ");
            return JSON.toJSONString(new WaterSensor(strings[0], Long.parseLong(strings[1]), Integer.parseInt(strings[2])));
        });

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");

        // 课堂版
//        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>("sensor_sink",
//                new SimpleStringSchema(),
//                properties);

        // 1.12版官网写法报错
//        FlinkKafkaProducer flinkKafkaProducer = new FlinkKafkaProducer<>(
//                "my-topic",                  // target topic
//                new SimpleStringSchema(),    // serialization schema
//                properties,                  // producer config
//                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);// fault-tolerance

        // 把官网代码替换如下：测试结果可以正常写入Kafka
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                "sensor_sink",
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        return new ProducerRecord<byte[], byte[]>("sensor_sink",
                                "20210812".getBytes(StandardCharsets.UTF_8),
                                element.getBytes(StandardCharsets.UTF_8));
                    }
                },
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

        jsonStrDS.addSink(producer);

        env.execute();
    }
}
