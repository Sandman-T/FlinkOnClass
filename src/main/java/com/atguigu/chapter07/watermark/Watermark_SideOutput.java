package com.atguigu.chapter07.watermark;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @description: 使用侧输出流把一个流拆成多个流
 * @create_time: 16:37 2021/8/12
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 * 需求: 采集监控传感器水位值，将水位值高于5cm的值输出到side output
 */
public class Watermark_SideOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> result = env.socketTextStream("hadoop102", 9527)
                .map(line -> {
                    String[] fields = line.split(" ");
                    return new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2]));
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        out.collect(value);

                        if (value.getVc() > 5) {
                            ctx.output(new OutputTag<String>("报警") {
                            }, value.toString());
                        }
                    }
                });

        result.print("主流：");
        result.getSideOutput(new OutputTag<String>("报警"){}).print("报警");

        env.execute();
    }
}
