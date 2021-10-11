package com.atguigu.preview.chapter07;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @description: 键控状态之定时器练习
 * @create_time: 15:20 2021/8/8
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 * 监控水位传感器的水位值，如果水位值在五秒钟之内连续上升，则报警，
 * 并将报警信息输出到侧输出流。
 */
public class Flink_KeyedStateExercise {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env
                .socketTextStream("hadoop102", 9527)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] words = value.split(",");
                        return new WaterSensor(words[0], Long.parseLong(words[1]), Integer.parseInt(words[2]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
                );

        SingleOutputStreamOperator<String> result = waterSensorDS.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    private ValueState<Integer> vcState;
                    private ValueState<Long> tsState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        vcState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastVC", Integer.class, Integer.MIN_VALUE));
                        tsState = getRuntimeContext().getState(new ValueStateDescriptor<>("ts", Long.class, Long.MIN_VALUE));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        if (value.getVc() > vcState.value()) {
                            if (tsState.value() == Long.MIN_VALUE) {
                                System.out.println("注册定时器。。。" + (value.getTs() + 5000L));
                                ctx.timerService().registerEventTimeTimer(value.getTs() + 5000L);
                                tsState.update(value.getTs() + 5000L);
                            }
                        } else {
                            System.out.println("删除定时器。。。" + tsState.value());
                            ctx.timerService().deleteEventTimeTimer(tsState.value());
                        }

                        vcState.update(value.getVc());
                        System.out.println("水位状态：=" + vcState.value());
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println("触发定时器。。。");
                        ctx.output(new OutputTag<String>("outSide") {
                                   },
                                "传感器=" + ctx.getCurrentKey() + "在ts=" + timestamp + "报警！！！！！！");

                        tsState.clear();
                    }

                    @Override
                    public void close() throws Exception {
                        vcState.clear();
                        tsState.clear();
                    }
                });

        result.print("主流");
        result.getSideOutput(new OutputTag<String>("sideOut") {
        }).print("报警信息");

        env.execute();

    }
}
