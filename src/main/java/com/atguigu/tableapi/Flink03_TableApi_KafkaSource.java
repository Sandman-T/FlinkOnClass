package com.atguigu.tableapi;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @description:
 * @create_time: 17:20 2021/8/17
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink03_TableApi_KafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", "BIGINT")
                .field("vc", "INT");

        tableEnv.connect(
                new Kafka()
                        .topic("source_sensor")
                        .version("universal")
                        .startFromLatest()
                        .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
                        .property(ConsumerConfig.GROUP_ID_CONFIG, "FlinkTableAPI")
        )
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        Table table = tableEnv.from("sensor")
                .groupBy($("id"))
                .select($("id"), $("ts").count().as("tscnt"), $("vc").sum().as("vcsum"));

        DataStream<Tuple2<Boolean, Row>> dataStream = tableEnv.toRetractStream(table, Row.class);

        dataStream.print();

        env.execute();

    }
}
