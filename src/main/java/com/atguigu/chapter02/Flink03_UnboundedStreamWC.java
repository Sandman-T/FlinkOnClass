package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @description:
 * @create_time: 13:17 2021/8/6
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink03_UnboundedStreamWC {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

//        env.setParallelism(43); // 可以

        env.setParallelism(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setRestartStrategy(RestartStrategies.fallBackRestart());
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9527);

        SingleOutputStreamOperator<Tuple2<String, Long>> word2oneDS = socketTextStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });

        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = word2oneDS.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedStream
                .sum(1);

        result.print();
        env.execute();
    }
}
