package com.atguigu.chapter07.window;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @description:
 * @create_time: 16:54 2021/8/10
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink09_Window_Fun_Apply {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9527);

        KeyedStream<WaterSensor, String> keyedStream = source.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
                String[] fields = value.split(",");
                out.collect(new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2])));
            }
        }).keyBy(WaterSensor::getId);

        // TODO process
        keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new WindowFunction<WaterSensor, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<WaterSensor> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                        System.out.println("apply...");
                        System.out.println(window.getStart() + ">>>" + window.getEnd());
                        int sumVC = 0;
                        for (WaterSensor element : input) {
                            sumVC += element.getVc();
                        }
                        out.collect(Tuple2.of(key, sumVC));
                    }
                }).print();

        env.execute();
    }
}
