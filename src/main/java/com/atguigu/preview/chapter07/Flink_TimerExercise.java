package com.atguigu.preview.chapter07;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @description: 定时器练习
 * @create_time: 14:06 2021/8/8
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 * 监控水位传感器的水位值，如果水位值在五秒钟之内连续上升，则报警，
 * 并将报警信息输出到侧输出流。
 */
public class Flink_TimerExercise {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // 注意：这里的泛型必须要指定，否则出现一大串红色报错
        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs();
                    }
                });
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
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forMonotonousTimestamps() // 有序递增的watermark 1ms
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
                );

        SingleOutputStreamOperator<String> process = waterSensorDS.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    private int lastVC = Integer.MIN_VALUE;
                    // 用来记录定时器时间
                    private Long timerTS = Long.MIN_VALUE;

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        if (value.getVc() > lastVC) {
                            if (timerTS == Long.MIN_VALUE) { // 第一次需要创建定时器
                                System.out.println("注册定时器。。。");
                                timerTS = value.getTs() + 5000L;
                                ctx.timerService().registerEventTimeTimer(value.getTs() + 5000L);
                            }
                        } else {
                            System.out.println("删除定时器。。。");
                            ctx.timerService().deleteEventTimeTimer(timerTS);
                            timerTS = Long.MIN_VALUE;
                        }
                        lastVC = value.getVc();
                    }

                    // 注意因为有个1ms的watermark存在，所以是触发创建定时器的time + 5s + 1ms时，才会触发定时器
                    // 而且触发的这次也必须要上升才行
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        ctx.output(new OutputTag<String>("sideOut") {
                                   },
                                ctx.getCurrentKey() + "报警！！！！！！"
                        );
                        // 重置定时器时间
//                        timerTS = Long.MIN_VALUE;
                    }
                });

        process.print("主流：");

        process.getSideOutput(new OutputTag<String>("sideOut") {})
                .print("报警：");


        env.execute();


    }
}
