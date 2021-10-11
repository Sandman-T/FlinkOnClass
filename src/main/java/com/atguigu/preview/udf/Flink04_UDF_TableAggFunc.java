package com.atguigu.preview.udf;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @description:
 * @create_time: 22:52 2021/8/10
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink04_UDF_TableAggFunc {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取文件得到DataStream
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.createTemporaryView("sensor", waterSensorDataStreamSource);

        tableEnv.from("sensor")
                .groupBy($("id"))
                .flatAggregate(call(VCTop2UDATF.class, $("vc")).as("value", "rank"))
                .select($("id"), $("value"), $("rank"))
                .execute()
                .print();

    }


    // 定义一个类当做累加器，并声明第一和第二这两个值
    public static class VCTop2 {
        public Integer first = Integer.MIN_VALUE;
        public Integer second = Integer.MIN_VALUE;
    }

    // 自定义UDATF函数（多进多出）,求每个WaterSensor中最高的两个水位值
    public static class VCTop2UDATF extends TableAggregateFunction<Tuple2<Integer, Integer>, VCTop2> {
        @Override
        public VCTop2 createAccumulator() {
            VCTop2 acc = new VCTop2();
            acc.first = Integer.MIN_VALUE; 
            acc.second = Integer.MIN_VALUE;
            return acc;
        }

        public void accumulate (VCTop2 acc, Integer vc) {
            if (vc > acc.first) {
                acc.second = acc.first;
                acc.first = vc;
            } else if (vc > acc.second) {
                acc.second = vc;
            }
        }

        public void emitValue(VCTop2 acc, Collector<Tuple2<Integer, Integer>> out) {
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, 2));
            }
        }

    }
}
