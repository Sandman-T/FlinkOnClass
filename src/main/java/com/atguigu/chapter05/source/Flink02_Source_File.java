package com.atguigu.chapter05.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description:
 * @create_time: 15:30 2021/8/7
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink02_Source_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从本地读取
        String path = "";

        // 从HDFS读取
        String hdfsPath = "hdfs://hadoop102:8020/input/kafkaCommand.txt";
        DataStreamSource<String> streamSource = env.readTextFile(hdfsPath);

        streamSource.print();


        env.execute();
    }


}
