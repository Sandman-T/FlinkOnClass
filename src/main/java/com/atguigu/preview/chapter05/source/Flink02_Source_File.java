package com.atguigu.preview.chapter05.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description: 从文件中读取数据
 * @create_time: 20:25 2021/8/5
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink02_Source_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String inputPath = "src\\main\\resources\\word.txt";
        env.readTextFile(inputPath)
                .print();
        env.execute();
    }
}
