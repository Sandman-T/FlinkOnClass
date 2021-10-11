package com.atguigu.chapter05.transform;

import com.atguigu.preview.bean.WaterSensor;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @description:
 * @create_time: 11:31 2021/8/9
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink10_Reduce {
//    @SneakyThrows(Exception.class) Lombok异常处理的注解
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));


        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromCollection(waterSensors);

        waterSensorDataStreamSource.keyBy(WaterSensor::getId)
                .reduce((value1, value2) -> new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc()) )
                .print();

        env.execute();
    }
}
