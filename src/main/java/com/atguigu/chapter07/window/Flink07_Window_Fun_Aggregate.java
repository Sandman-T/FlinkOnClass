package com.atguigu.chapter07.window;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @description:
 * @create_time: 16:54 2021/8/10
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink07_Window_Fun_Aggregate {
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

        // TODO aggregate
        keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .aggregate(new AggregateFunction<WaterSensor, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> createAccumulator() {
                        System.out.println("创建累加器。。。");
                        return Tuple2.of(null, 0);
                    }

                    @Override
                    public Tuple2<String, Integer> add(WaterSensor value, Tuple2<String, Integer> accumulator) {
                        System.out.println("累加。。。");
                        return Tuple2.of(value.getId(), value.getVc() + accumulator.f1);
                    }

                    @Override
                    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
                        System.out.println("获取结果。。。");
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                        System.out.println("分区间合并累加器。。。");
                        return Tuple2.of(a.f0, a.f1 + b.f1);
                    }
                }).print();

        env.execute();
    }
}
