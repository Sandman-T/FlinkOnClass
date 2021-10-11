package com.atguigu.udf;

import com.atguigu.bean.WaterSensor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;


/**
 * @description: 自定义表聚合函数：求每个id的top2的vc及其序号
 * @create_time: 11:08 2021/8/18
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink04_UDF_UDTAF {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.将流转为动态表
        Table table = tableEnv.fromDataStream(waterSensorDataStreamSource);

        //TODO a.不注册直接使用
//        table.groupBy($("id"))
//                .flatAggregate(call(Top2Fun.class, $("vc")))
//                .select($("id"), $("f0"), $("f1"))
//                .execute()
//                .print();

        //TODO b.先注册再使用
        tableEnv.createTemporarySystemFunction("myFun", Top2Fun.class);
        //Table API
        table.groupBy($("id"))
                .flatAggregate(call("myFun", $("vc")).as("value", "rank"))
                .select($("id"), $("value"), $("rank"))
                .execute()
                .print();

        //SQL
        tableEnv.createTemporaryView("sensor", table);


    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Top2Acc {
        private Integer first;
        private Integer second;
    }


    public static class Top2Fun extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Acc> {

        @Override
        public Top2Acc createAccumulator() {
            return new Top2Acc(Integer.MIN_VALUE, Integer.MIN_VALUE);
        }

        public void accumulate(Top2Acc acc, Integer vc) {
            if (vc > acc.first) {
                acc.second = acc.first;
                acc.first = vc;
            } else if (vc > acc.second) {
                acc.second = vc;
            }

        }


        public void emitValue(Top2Acc acc, Collector<Tuple2<Integer, Integer>> out) {
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, 2));
            }

        }


    }
}
