package com.atguigu.chapter05.sink;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;

/**
 * @description:
 * @create_time: 10:03 2021/8/8
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink04_Sink_MySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        // 1.10
        env.fromCollection(waterSensors)
                .addSink(new MySQLSink());


        // 1.12 : 导入 flink-connector-jdbc_2.11
        env.fromCollection(waterSensors)
                .addSink(JdbcSink.sink(
                        "insert into sensor values (?,?,?)",
                        (PreparedStatement pst, WaterSensor element) -> {
                            pst.setString(1, element.getId());
                            pst.setLong(2, element.getTs());
                            pst.setInt(3, element.getVc());
                        },
                        new JdbcOptions.Builder()
                                .setDBUrl("jdbc:mysql://hadoop102:3306/test")
                                .setDriverName("com.mysql.jdbc.Driver")
                                .setUsername("root")
                                .setPassword("root")
                                .build()
                ));


        env.execute();
    }

    public static class MySQLSink extends RichSinkFunction<WaterSensor> {
        private Connection connection;
        private PreparedStatement pst;
        private String sql;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection(
                    "jdbc:mysql://hadoop102:3306/test",
                    "root",
                    "root"
            );
            sql = "insert into sensor values (?, ?, ?)";
            pst = connection.prepareStatement(sql);
        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            pst.setString(1, value.getId());
            pst.setLong(2, value.getTs());
            pst.setInt(3, value.getVc());
            pst.executeUpdate();
        }

        @Override
        public void close() throws Exception {
            pst.close();
            connection.close();
        }
    }
}
