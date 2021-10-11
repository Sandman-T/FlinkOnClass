package com.atguigu.chapter05.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @description:
 * @create_time: 11:27 2021/8/9
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink08_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> dataStreamSource1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> dataStreamSource2 = env.fromElements(11, 22, 33, 44, 55);

        DataStream<Integer> union = dataStreamSource1.union(dataStreamSource2);

        union.process(new ProcessFunction<Integer, String>() {
            @Override
            public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
                out.collect("integer:" + value);
            }
        }).print();

        env.execute();
    }
}
