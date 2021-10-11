package com.atguigu.tableapi;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @description:
 * @create_time: 17:20 2021/8/17
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink05_TableApi_KafkaSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 把流转换成table
        Table table = tableEnv.fromDataStream(waterSensorDataStreamSource);


        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", "BIGINT")
                .field("vc", "INT");

        tableEnv.connect(
                new Kafka()
                        .topic("sink_sensor")
                        .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
        )
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        // 查询table对象的数据写入kafka sink
        table.select($("id"), $("ts"), $("vc"))
                .executeInsert("sensor");

    }
}
