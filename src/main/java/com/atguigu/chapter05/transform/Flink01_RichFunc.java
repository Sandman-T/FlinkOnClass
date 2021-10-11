package com.atguigu.chapter05.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * @description:
 * @create_time: 13:23 2021/8/12
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink01_RichFunc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.readTextFile("src/main/resources/sensor.txt")
                .map(new RichMapFunction<String, WaterSensor>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("open...");
                    }

                    @Override
                    public WaterSensor map(String value) throws Exception {
                        System.out.println("map...");
                        String[] fields = value.split(",");
                        return  new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2]));
                    }

                    @Override
                    public void close() throws Exception {
                        System.out.println("close...");
                    }
                })
                .print();


        env.execute();
    }
}
