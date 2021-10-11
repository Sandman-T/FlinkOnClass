package com.atguigu.preview.tableapi;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @description:
 * @create_time: 21:17 2021/8/11
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Sink_Kafka02 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        tableEnv.connect(
                new Kafka()
                .version("universal")
                .topic("flink_sql_sink_sensor")
                .sinkPartitionerRoundRobin()
                .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
//                .property(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
//                .property(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()) // org.apache.kafka.common.errors.SerializationException: Can't convert value of class [B to class org.apache.kafka.common.serialization.StringSerializer specified in value.serializer
                        .property(ProducerConfig.ACKS_CONFIG, "all")
        )
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        tableEnv.fromDataStream(source)
                .select($("id"), $("ts"), $("vc"))
                .executeInsert("sensor");

    }
}
