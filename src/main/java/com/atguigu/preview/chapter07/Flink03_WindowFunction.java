package com.atguigu.preview.chapter07;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @description:
 * @create_time: 19:55 2021/8/6
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink03_WindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<Tuple2<String, Long>, String> keyedStream = env.socketTextStream("hadoop102", 9527)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        Arrays.stream(value.split(" "))
                                .forEach(word -> out.collect(Tuple2.of(word, 1L)));
                    }
                })
                .keyBy(in -> in.f0);

        // TODO 滚动窗口
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> window01 = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        // TODO 滑动窗口
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> window02 = keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));

        // TODO WindowFunction

        // ReduceFunction(增量聚合函数----不会改变数据的类型)
/*        SingleOutputStreamOperator<Tuple2<String, Long>> reduce = window01.reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                System.out.println(value1 + " ----- " + value2);
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });*/
//        reduce.print("reduce:");

        // AggregateFunction(增量聚合函数----可以改变数据的类型)
/*        SingleOutputStreamOperator<Long> aggregate = window01.aggregate(new AggregateFunction<Tuple2<String, Long>, Long, Long>() {
            // 创建累加器: 初始化中间值  每个key创建一次
            @Override
            public Long createAccumulator() {
                System.out.println("createAccumulator");
                return 0L;
            }

            // 每个元素add一次
            @Override
            public Long add(Tuple2<String, Long> value, Long accumulator) {
                System.out.println("add");
                return accumulator + value.f1;
            }

            @Override
            public Long getResult(Long accumulator) {
                System.out.println("getResult");
                return accumulator;
            }

            // 累加器的合并: 只有会话窗口才会调用
            @Override
            public Long merge(Long a, Long b) {
                System.out.println("merge");
                return a + b;
            }
        });

        aggregate.print("agg:");*/

        // ProcessWindowFunction(全窗口函数)
        SingleOutputStreamOperator<Tuple2<String, Long>> process = window01.process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>() {
            @Override
            public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Long>> out) throws Exception {
                Long count = 0L;
                for (Tuple2<String, Long> element : elements) {
                    count += element.f1;
                }
                out.collect(Tuple2.of(key, count));
            }
        });
        process.print("process:");


        env.execute("time window test");
    }
}
