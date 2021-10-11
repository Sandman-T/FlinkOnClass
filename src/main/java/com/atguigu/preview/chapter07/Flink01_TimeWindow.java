package com.atguigu.preview.chapter07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
public class Flink01_TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        System.out.println("\\W+");

        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = env.socketTextStream("hadoop102", 9527)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        Arrays.stream(value.split(" "))
                                .forEach(word -> out.collect(Tuple2.of(word, 1L)));
                    }
                })
                .keyBy(0);

        // TODO 滚动窗口
        WindowedStream<Tuple2<String, Long>, Tuple, TimeWindow> window01 = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(6)));


        // TODO 滑动窗口
        WindowedStream<Tuple2<String, Long>, Tuple, TimeWindow> window02 = keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(6), Time.seconds(3)));


        // TODO 会话窗口
        // 静态gap
        WindowedStream<Tuple2<String, Long>, Tuple, TimeWindow> window03 = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));
        // 动态gap
        WindowedStream<Tuple2<String, Long>, Tuple, TimeWindow> window04 = keyedStream.window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple2<String, Long>>() {
            @Override
            public long extract(Tuple2<String, Long> element) {
                return element.f0.length() * 1000;
            }
        }));

        // TODO 全局窗口
        WindowedStream<Tuple2<String, Long>, Tuple, GlobalWindow> window05 = keyedStream.window(GlobalWindows.create());


        env.execute("time window test");
    }
}
