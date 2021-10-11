package com.atguigu.chapter09;

import com.atguigu.bean.LoginEvent;
import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @description: 统计创建订单到下单中间超过15分钟的超时数据以及正常的数据
 * @create_time: 13:21 2021/8/14
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink02_CEP_Project_OrderWatch {
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("src/main/resources/LoginLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(Long.parseLong(fields[0]), fields[1], fields[2], Long.parseLong(fields[3]) * 1000);
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<OrderEvent>) (element, recordTimestamp) -> element.getEventTime()
                                )
                );

        KeyedStream<OrderEvent, Long> keyedStream = orderEventDS.keyBy(OrderEvent::getOrderId);


        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("start")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .next("next")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));

        SingleOutputStreamOperator<String> result = CEP.pattern(keyedStream, pattern)
                .select(
                        new OutputTag<String>("timeout") {
                        },
                        new PatternTimeoutFunction<OrderEvent, String>() {
                            @Override
                            public String timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
                                return map.toString();
                            }
                        },
                        new PatternSelectFunction<OrderEvent, String>() {
                            @Override
                            public String select(Map<String, List<OrderEvent>> map) throws Exception {
                                return map.toString();
                            }
                        }
                );

        result.getSideOutput(new OutputTag<String>("timeout"){}).print("超时数据：");
        result.print("正常数据：");

        env.execute();
    }
}
