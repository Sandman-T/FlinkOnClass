package com.atguigu.chapter05.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @description:
 * @create_time: 17:20 2021/8/7
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink04_Source_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "TEST");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        DataStreamSource<String> streamSource = env.addSource(
                new FlinkKafkaConsumer<String>("sensor",
                        new SimpleStringSchema(),
                        properties)
        );

        streamSource.print();

        env.execute();
    }
}
