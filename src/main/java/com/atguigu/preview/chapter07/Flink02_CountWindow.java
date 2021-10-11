package com.atguigu.preview.chapter07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
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
public class Flink02_CountWindow {
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
        WindowedStream<Tuple2<String, Long>, Tuple, GlobalWindow> window01 = keyedStream.countWindow(5);

        // TODO 滑动窗口
        WindowedStream<Tuple2<String, Long>, Tuple, GlobalWindow> window02 = keyedStream.countWindow(10, 2);

        env.execute("time window test");
    }
}
