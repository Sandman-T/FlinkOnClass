package com.atguigu.chapter07.timer;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @description: 定时器练习：监控水位传感器的水位值，如果水位值在五秒钟之内连续上升，则报警，并将报警信息输出到侧输出流
 * @create_time: 17:00 2021/8/11
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 *
 */
public class Flink03_Timer_Exercise {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9527);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = source.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
                String[] fields = value.split(",");
                out.collect(new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2])));
            }
        });

        // 当有多个key时，会存在bug，因为定时器是分key的，但是用来存放水位和定时器时间的变量每个并行度共享一个
        SingleOutputStreamOperator<WaterSensor> result = waterSensorDS.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    // 存放上一个水位
                    private Integer lastVC = Integer.MIN_VALUE;
                    // 存放定时器的时间
                    private Long timer = Long.MIN_VALUE;

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        // 判断当前水位较上一个水位，是否上升
                        if (value.getVc() > lastVC) {
                            // 水位上涨
                            if (timer == Long.MIN_VALUE) { // 判断定时器状态
                                // 如果没有创建则创建
                                timer = ctx.timerService().currentProcessingTime() + 5000L;
                                ctx.timerService().registerProcessingTimeTimer(timer);
                                System.out.println("key:" + ctx.getCurrentKey() + ",创建定时器：" + timer);
                            }
                        } else {
                            // 水位没有上升：删除定时器，重置timer数值
                            System.out.println("key:" + ctx.getCurrentKey() + ",删除定时器：" + timer);
                            ctx.timerService().deleteProcessingTimeTimer(timer);
                            timer = Long.MIN_VALUE;
                        }
                        // 无论如何都要更新上一次的水位
                        lastVC = value.getVc();
                        out.collect(value);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                        System.out.println("key:" + ctx.getCurrentKey() + ",触发定时器：" + timer);
                        ctx.output(new OutputTag<String>("报警"){}, "水位连续5s上涨");

                        // 清理timer
                        timer = Long.MIN_VALUE;
                    }
                });

        result.print("主流：");
        result.getSideOutput(new OutputTag<String>("报警"){}).print("报警：");


        env.execute();


    }
}
