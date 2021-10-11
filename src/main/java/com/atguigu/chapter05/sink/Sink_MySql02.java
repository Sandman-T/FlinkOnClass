package com.atguigu.chapter05.sink;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @description:
 * @create_time: 14:42 2021/8/12
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Sink_MySql02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("sensor_1", 1547718199L, 35),
                new WaterSensor("sensor_6", 1547718201L, 15),
                new WaterSensor("sensor_7", 1547718202L, 6),
                new WaterSensor("sensor_10", 1547718205L, 38)
        );

        source.addSink(new RichSinkFunction<WaterSensor>() {
            private Connection connection;
            private PreparedStatement pst;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "root");
                String sql = "insert into sensor(id,ts,vc) values (?,?,?) on duplicate key update id = values(id),ts=values(ts), vc=values(vc)";
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
        });

        env.execute();
    }
}
