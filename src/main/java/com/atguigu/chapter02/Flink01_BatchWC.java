package com.atguigu.chapter02;

import lombok.val;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @description:
 * @create_time: 11:39 2021/8/6
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink01_BatchWC {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSource<String> lineDS = env.readTextFile("src/main/resources/word.txt");

        FlatMapOperator<String, Tuple2<String, Long>> word2oneDS = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 2L));
                }
            }
        });

//        UnsortedGrouping<Tuple2<String, Long>> wordGroupDS = word2oneDS.groupBy(new KeySelector<Tuple2<String, Long>, String>() {
//
//            @Override
//            public String getKey(Tuple2<String, Long> value) throws Exception {
//                return value.f0;
//            }
//        });
//
//        wordGroupDS.first(1);

        UnsortedGrouping<Tuple2<String, Long>> wordGroupDS = word2oneDS.groupBy(0);

        AggregateOperator<Tuple2<String, Long>> wcDS = wordGroupDS.sum(1);

        wcDS.print();
    }
}
