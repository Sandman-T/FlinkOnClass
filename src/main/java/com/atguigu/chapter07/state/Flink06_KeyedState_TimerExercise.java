package com.atguigu.chapter07.state;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @description:
 * @create_time: 21:05 2021/8/13
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink06_KeyedState_TimerExercise {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9527);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = source.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
                String[] fields = value.split(" ");
                out.collect(new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2])));
            }
        });

        // 当有多个key时，会存在bug，因为定时器是分key的，但是用来存放水位和定时器时间的变量每个并行度共享一个
        SingleOutputStreamOperator<WaterSensor> result = waterSensorDS.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    // 存放上一个水位
                    private ValueState<Integer> lastVCState;
                    // 存放定时器的时间
                    private ValueState<Long> timerState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVCState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVC", Integer.class, Integer.MIN_VALUE));
                        timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class, Long.MIN_VALUE));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        // 判断当前水位较上一个水位，是否上升
                        if (value.getVc() > lastVCState.value()) {
                            // 水位上涨
                            if (timerState.value() == Long.MIN_VALUE) { // 判断定时器状态
                                // 如果没有创建则创建
                                timerState.update(ctx.timerService().currentProcessingTime() + 5000L); ;
                                ctx.timerService().registerProcessingTimeTimer(timerState.value());
                                System.out.println("key:" + ctx.getCurrentKey() + ",创建定时器：" + timerState.value());
                            }
                        } else {
                            // 水位没有上升：删除定时器，重置timer数值
                            if (timerState.value() != Long.MIN_VALUE) {
                                System.out.println("key:" + ctx.getCurrentKey() + ",删除定时器：" + timerState.value());
                                ctx.timerService().deleteProcessingTimeTimer(timerState.value());
                                timerState.clear();
                            }
                        }
                        // 无论如何都要更新上一次的水位
                        lastVCState.update(value.getVc());
                        out.collect(value);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                        System.out.println("key:" + ctx.getCurrentKey() + ",触发定时器：" + timerState.value());
                        ctx.output(new OutputTag<String>("报警"){}, "水位连续5s上涨");

                        // 清理timer
                        timerState.clear();
                    }
                });

        result.print("主流：");
        result.getSideOutput(new OutputTag<String>("报警"){}).print("报警：");


        env.execute();
    }
}
