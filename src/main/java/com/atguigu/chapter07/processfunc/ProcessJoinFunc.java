package com.atguigu.chapter07.processfunc;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @description:
 * @create_time: 17:10 2021/8/12
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class ProcessJoinFunc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> input01 = env.socketTextStream("hadoop102", 9527)
                .map(line -> {
                    String[] fields = line.split(" ");
                    return new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2]));
                });

        SingleOutputStreamOperator<WaterSensor> input02 = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(" ");
                    return new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2]));
                });
        input01.join(input02)
                .where(WaterSensor::getId)
                .equalTo(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .apply(new JoinFunction<WaterSensor, WaterSensor, WaterSensor>() {
                    @Override
                    public WaterSensor join(WaterSensor first, WaterSensor second) throws Exception {
                        return new WaterSensor(first.getId(), first.getTs(), first.getVc() + second.getVc()) ;
                    }
                })
                .print();

        env.execute();
    }
}
