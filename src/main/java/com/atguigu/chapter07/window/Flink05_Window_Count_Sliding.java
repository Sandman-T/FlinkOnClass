package com.atguigu.chapter07.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @description:
 * @create_time: 16:48 2021/8/10
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink05_Window_Count_Sliding {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9527);

        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = source.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] fields = value.split(",");
                out.collect(Tuple2.of(fields[0], 1L));
            }
        }).keyBy(0);

        WindowedStream<Tuple2<String, Long>, Tuple, GlobalWindow> window = keyedStream.countWindow(5, 2);

        window.sum(1).print();

        env.execute();

    }
}
