package com.atguigu.chapter07.timer;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @description:
 * @create_time: 19:18 2021/8/11
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink02_Timer_Event_Parallel {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);


        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9527);


        SingleOutputStreamOperator<WaterSensor> waterSensorDS = source.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] fields = value.split(",");
                return new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2]));
            }
        });

        // TODO 分配一个乱序的watermark，指定最大乱序程度为2s，并指定事件时间戳
        SingleOutputStreamOperator<WaterSensor> watermarks = waterSensorDS
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
                );

        watermarks.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        //TODO 注册一个基于处理时间的定时器
                        System.out.println("key:" + ctx.getCurrentKey() + ",注册一个定时器" + (ctx.timestamp() + 5000L));
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println("key:" + ctx.getCurrentKey() + ",触发一个定时器" );
                        out.collect("");
                    }

                }).print();

        env.execute();
    }
}
