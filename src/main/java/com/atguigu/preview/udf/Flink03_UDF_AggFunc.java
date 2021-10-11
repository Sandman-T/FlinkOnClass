package com.atguigu.preview.udf;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @description:
 * @create_time: 22:37 2021/8/10
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink03_UDF_AggFunc {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取文件得到DataStream
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.createTemporaryView("sensor", waterSensorDataStreamSource);

        tableEnv.from("sensor")
                .groupBy($("id"))
                .select($("id"), call(MyAvg.class, $("vc")))
                .execute()
                .print();

        //3.将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDataStreamSource);


        //4先注册再使用
        tableEnv.createTemporarySystemFunction("myavg", MyAvg.class);

        //TableAPI
        table.groupBy($("id"))
                .select($("id"),call("myavg",$("vc")))
                .execute()
                .print();

        //SQL
        tableEnv.executeSql("select id, myavg(vc) from "+ table +" group by id").print();


    }

    public static class MyAvg extends AggregateFunction<Double, Tuple2<Long, Integer>> {
        @Override
        public Double getValue(Tuple2<Long, Integer> accumulator) {
            return  accumulator.f0 * 1.0 / accumulator.f1;
        }

        @Override
        public Tuple2<Long, Integer> createAccumulator() {
            return Tuple2.of(0L, 0);
        }

        public void accumulate(Tuple2<Long, Integer> acc, Integer vc) {
            acc.f0 += vc;
            acc.f1 += 1;
        }

    }
}
