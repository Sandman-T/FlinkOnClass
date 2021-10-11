package com.atguigu.preview.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @description:
 * @create_time: 20:52 2021/8/11
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Source_Kafka02 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema schema = new Schema().field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        tableEnv.connect(
                new Kafka()
                        .version("universal")
                        .topic("flink_sql_source_sensor")
                        .startFromLatest()
                        .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
                        .property(ConsumerConfig.GROUP_ID_CONFIG, "flink")
                        .property(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
        )
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        tableEnv.from("sensor")
                .groupBy($("id"))
                .aggregate($("vc").sum().as("sum_vc"))
                .select($("id"), $("sum_vc"))
                .execute()
                .print();

    }
}
