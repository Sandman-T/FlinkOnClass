package com.atguigu.tableapi;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @description:
 * @create_time: 17:14 2021/8/17
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink04_TableApi_FileSink {
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

        Table table = tableEnv.fromDataStream(waterSensorDataStreamSource);

        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", "BIGINT")
                .field("vc", "INT");

        tableEnv.connect(new FileSystem().path("output/sensor1.txt"))
                .withFormat(new Csv().fieldDelimiter(','))
                .withSchema(schema)
                .createTemporaryTable("sensor");


        table.where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"))
                .executeInsert("sensor");

//        env.execute();
    }
}
