package com.atguigu.preview.udf;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @description: 自定义UDTF函数将传入的id按照下划线炸裂成两条数据
 * @create_time: 22:02 2021/8/10
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink02_UDF_TableFunc {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取文件得到DataStream
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1_test", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporaryView("sensor", waterSensorDataStreamSource);

        tableEnv.from("sensor")
                .joinLateral(call(MySplit.class, $("id")))
                .select($("id"), $("ts"), $("vc"), $("word"), $("length"))
                .execute()
                .print();

    }

    // hint暗示，主要作用为类型推断时使用
    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
    public static class MySplit extends TableFunction<Row> {
        public void eval (String str) {
            for (String s : str.split("_")) {
                collect(Row.of(s, s.length()));
            }
        }
    }
}
