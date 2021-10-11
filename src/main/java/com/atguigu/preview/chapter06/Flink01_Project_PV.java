package com.atguigu.preview.chapter06;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description:
 * @create_time: 10:53 2021/8/8
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink01_Project_PV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> lineDS = env.readTextFile("src/main/resources/UserBehavior.csv");



        env.execute();
    }
}
