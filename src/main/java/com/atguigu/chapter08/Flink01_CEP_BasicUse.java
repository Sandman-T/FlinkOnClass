package com.atguigu.chapter08;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @create_time: 7:04 2021/8/14
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink01_CEP_BasicUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        SingleOutputStreamOperator<WaterSensor> source = env.fromElements(
                new WaterSensor("sensor_1", 1607527992000L, 20),
                new WaterSensor("sensor_3", 1607527995000L, 80),
                new WaterSensor("sensor_1", 1607527994000L, 50),
                new WaterSensor("sensor_2", 1607527998000L, 10),
                new WaterSensor("sensor_2", 1607527999000L, 30)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> element.getTs())
        );

        // 1.定义模式
//        Pattern<WaterSensor, WaterSensor> pattern = Pattern.<WaterSensor>begin("start")
//                .where(new SimpleCondition<WaterSensor>() {
//                    @Override
//                    public boolean filter(WaterSensor value) throws Exception {
//                        return "sensor_1".equals(value.getId());
//                    }
//                });

//        Pattern<WaterSensor, WaterSensor> pattern = Pattern.<WaterSensor>begin("start")
//                .where(new IterativeCondition<WaterSensor>() {
//                    @Override
//                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
//                        return "sensor_1".equals(waterSensor.getId());
//                    }
//                });

        Pattern<WaterSensor, WaterSensor> pattern1 = Pattern.<WaterSensor>begin("start")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
                .next("n1")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
//                .next("n2")
//                .where(new SimpleCondition<WaterSensor>() {
//                    @Override
//                    public boolean filter(WaterSensor value) throws Exception {
//                        return !"sensor_1".equals(value.getId());
//                    }
//                })
                ;


        // 2.在流上使用
        PatternStream<WaterSensor> patternStream = CEP.pattern(source, pattern1);

        // 3.获取匹配到的结果
        patternStream.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> map) throws Exception {
                return map.toString();
            }
        }).print();

        env.execute();
    }
}
