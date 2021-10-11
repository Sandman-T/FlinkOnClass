package com.atguigu.chapter08;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @description: 贪婪模式
 * @create_time: 7:04 2021/8/14
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink02_CEP_Greedy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> source = env.readTextFile("src/main/resources/sensor1.txt")
        .map(line -> {
            String[] fields = line.split(",");
            return new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2]));
        })
        .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> element.getTs())
        );

        // 1.定义模式
        Pattern<WaterSensor, WaterSensor> pattern = Pattern.<WaterSensor>begin("start")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
                .times(1, 3).greedy() //贪婪模式，会获取尽可能多的数据,当一个事件同时满足两个模式的时候起作用.
                .next("end")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return value.getVc() == 30;
                    }
                })
                ;


        // 2.在流上使用
        PatternStream<WaterSensor> patternStream = CEP.pattern(source, pattern);

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
