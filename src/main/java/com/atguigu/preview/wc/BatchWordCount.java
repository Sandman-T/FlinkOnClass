package com.atguigu.preview.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @description: 批处理实现word count
 * @create_time: 16:53 2021/8/5
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // TODO 创建批执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取离线数据
        String inputPath = "src\\main\\resources\\word.txt";
        DataSource<String> lineDataSource = env.readTextFile(inputPath);

        FlatMapOperator<String, Tuple2<String, Long>> word2oneDS = lineDataSource
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        word2oneDS.print();

        UnsortedGrouping<Tuple2<String, Long>> wordGroupDS = word2oneDS.groupBy(0);

        AggregateOperator<Tuple2<String, Long>> wordCountDS = wordGroupDS.sum(1);

        wordCountDS.print();

    }
}
