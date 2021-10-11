package com.atguigu.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @description:
 * @create_time: 20:19 2021/8/9
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Sink_MySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9527);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = source.map(line -> {
            String[] strings = line.split(" ");
            return new WaterSensor(strings[0], Long.parseLong(strings[1]), Integer.parseInt(strings[2]));
        });

        waterSensorDS.addSink(new MySQLSink());

        env.execute();
    }

    private static class MySQLSink extends RichSinkFunction<WaterSensor> {
        private Connection connection;
        private PreparedStatement pst;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
//            pst = connection.prepareStatement("insert into sensor values (?, ?, ?) ");
            String sql = "insert into sensor(id, ts, vc) values(?,?,?) on duplicate key update id = values(id),ts=values(ts), vc=values(vc)";
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
