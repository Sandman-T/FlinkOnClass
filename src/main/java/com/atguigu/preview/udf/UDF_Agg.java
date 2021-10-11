package com.atguigu.preview.udf;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @description:
 * @create_time: 6:26 2021/8/13
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class UDF_Agg {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> source = env.socketTextStream("hadoop102", 9527)
                .map(line -> {
                    String[] fields = line.split(" ");
                    return new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2]));
                });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.createTemporaryView("sensor", source);

        tableEnv.from("sensor")
                .groupBy($("id"))
                .aggregate(call(MyAvg.class, $("vc")).as("avg_vc"))
                .select($("id"), $("avg_vc"))
                .execute()
                .print();
    }

    public static class MyAvg extends AggregateFunction<Double, Tuple2<Long, Integer>> {
        @Override
        public Double getValue(Tuple2<Long, Integer> accumulator) {
            return accumulator.f0 * 1.0 / accumulator.f1;
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
