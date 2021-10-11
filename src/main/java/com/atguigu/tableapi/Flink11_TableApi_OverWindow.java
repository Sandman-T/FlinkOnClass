package com.atguigu.tableapi;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @description:
 * @create_time: 10:06 2021/8/18
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink11_TableApi_OverWindow {

    public static void main(String[] args) {
        // 1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.创建输入流
        SingleOutputStreamOperator<WaterSensor> source = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        ).assignTimestampsAndWatermarks(WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs();
                    }
                })
        );

        // 3.创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 4.把流转换成表，并指定时间
        Table table = tableEnv
                .fromDataStream(source, $("id"), $("ts"), $("vc"), $("et").rowtime());

        // 5.开窗：滚动窗口+处理时间
        table.window(Tumble.over(lit(3).second()).on($("pt")).as("w"))
                .groupBy($("id"), $("w"))
                .select($("id"), $("w").start(), $("w").end(), $("vc").sum())
                .execute()
                .print();

    }
}
