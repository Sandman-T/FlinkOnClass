package com.atguigu.preview.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @description: 流处理实现word count 之 有界流：读取一个文件
 * @create_time: 18:29 2021/8/5
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inputPath = "src\\main\\resources\\word.txt";
        DataStreamSource<String> lineDS = env.readTextFile(inputPath);

        SingleOutputStreamOperator<Tuple2<String, Long>> word2oneDS = lineDS.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 两种情况下返回值中key的泛型不一样
//        KeyedStream<Tuple2<String, Long>, String> wordGroupDS = word2oneDS.keyBy(in -> in.f0);
        KeyedStream<Tuple2<String, Long>, Tuple> wordGroupDS = word2oneDS.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Long>> wordCountDS = wordGroupDS.sum(1);

        wordCountDS.print();

        env.execute();
    }
}
