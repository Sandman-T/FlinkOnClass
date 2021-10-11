package com.atguigu.chapter05.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @description:
 * @create_time: 11:24 2021/8/9
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink07_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> stringDataStreamSource = env.fromElements("a", "b", "c", "d", "e");

        SingleOutputStreamOperator<String> process = integerDataStreamSource.connect(stringDataStreamSource)
                .process(new CoProcessFunction<Integer, String, String>() {
                    @Override
                    public void processElement1(Integer value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("integer:" + value);
                    }

                    @Override
                    public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("string:" + value);
                    }
                });

        process.print();
        env.execute();
    }
}
