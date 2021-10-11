package com.atguigu.preview.tableapi;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.AggregatedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @description:
 * @create_time: 22:20 2021/8/9
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink01_TableApi_BasicUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        Table sensor = tableEnv.fromDataStream(source, "id, ts, vc as waterline");
        Table select = sensor.select("id, waterline");


        Table sensor1 = tableEnv.fromDataStream(source,
                $("id"), $("ts"), $("vc").as("waterline"));
        Table select1 = sensor1.where($("id").isEqual("sensor_1"))
                .select($("id"), $("waterline"));

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(select1, Row.class);
        rowDataStream.print("where:");

        Table select2 = sensor1.groupBy($("id"))
                .aggregate($("waterline").sum().as("sum_vc"))
                .select($("id"), $("sum_vc"));
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(select2, Row.class);
        retractStream.print("agg:");

        env.execute();
    }
}
