package com.atguigu.chapter07.watermark;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @description: 无序事件时间的water mark + 滑动窗口
 * @create_time: 11:27 2021/8/11
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink03_WaterMark_Orderless_SlidingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
                        return element.getTs() * 1000;
                    }
                })
        );

        // 分组并开窗 滑动窗口
        watermarks.keyBy(WaterSensor::getId)
                .window(SlidingEventTimeWindows.of(Time.seconds(6), Time.seconds(3)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        String msg = "key:" + key + "的窗口：[" + context.window().getStart() + "," + context.window().getEnd()
                                + ") 共有：" + elements.spliterator().estimateSize() + "条数据";
                        out.collect(msg);
                    }
                }).print();

        env.execute();
    }
}
