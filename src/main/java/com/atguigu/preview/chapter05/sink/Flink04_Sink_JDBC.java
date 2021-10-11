package com.atguigu.preview.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;

/**
 * @description: 把数据写入Kafka
 * @create_time: 22:17 2021/8/5
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink04_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        waterSensors.add(new WaterSensor("sensor_3", 1607527998000L, 70));
        waterSensors.add(new WaterSensor("sensor_4", 1607527999000L, 20));


        env.fromCollection(waterSensors)
                .keyBy(WaterSensor::getId)
//                .sum("vc")
//                .print();
                .addSink(new MyRichSink());

        env.execute();

    }

    public static class MyRichSink extends RichSinkFunction<WaterSensor> {
        private Connection connection ;
        private PreparedStatement ps;

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open()"); // 每个并行度执行一次，和有没有keyBy没关系
            String url = "jdbc:mysql://hadoop102:3306/test";
            String user = "root";
            String passwd = "123456";
            connection = DriverManager.getConnection(url, user, passwd);

            //  on duplicate key update id = value(?), ts = value(?), vc = value(?)
            String sql = "insert into sensor values(?, ?, ?) ";
            ps = connection.prepareStatement(sql);
        }

        @Override
        public void close() throws Exception {
            System.out.println("close()"); // 每个并行度执行一次，和有没有keyBy没关系
            ps.close();
            connection.close();
        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            ps.setString(1, value.getId());
            ps.setLong(2, value.getTs());
            ps.setInt(3, value.getVc());

            ps.executeUpdate();
        }
    }

}
