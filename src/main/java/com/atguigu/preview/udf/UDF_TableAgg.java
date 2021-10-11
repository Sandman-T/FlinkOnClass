package com.atguigu.preview.udf;

import com.atguigu.preview.bean.WaterSensor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @description:
 * @create_time: 6:45 2021/8/13
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class UDF_TableAgg {
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
                .flatAggregate(call(MyTop2.class, $("vc")).as("highvc", "rank"))
//                .select($("id"), $("f0"), $("f1"))
                .select($("id"), $("highvc"), $("rank"))
                .execute()
                .print();

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Top2 {
        private Integer first;
        private Integer second;
    }


    // Tuple2<vc, rank> => (21, 1) (19, 2)
    public static class MyTop2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2> {
        @Override
        public Top2 createAccumulator() {
            return new Top2(Integer.MIN_VALUE, Integer.MIN_VALUE);
        }

        public void accumulate(Top2 acc, Integer vc) {
            if (vc > acc.first) {
                acc.second = acc.first;
                acc.first = vc;
            } else if (vc > acc.second) {
                acc.second = vc;
            }
        }

        // 赛马或者叫归并排序
        public void merge(Top2 acc, Iterable<Top2> accList) {
            for (Top2 another : accList) {
                accumulate(acc, another.first);
                accumulate(acc, another.second);
            }
        }

        // 返回结果
        public void emitValue(Top2 acc, Collector<Tuple2<Integer, Integer>> out) {
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, 2));
            }

        }


    }
}
