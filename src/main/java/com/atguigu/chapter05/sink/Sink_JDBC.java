package com.atguigu.chapter05.sink;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @description:
 * @create_time: 20:33 2021/8/9
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Sink_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9527);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = source.map(line -> {
            String[] strings = line.split(" ");
            return new WaterSensor(strings[0], Long.parseLong(strings[1]), Integer.parseInt(strings[2]));
        });

        // 官网写法




        waterSensorDS.addSink(JdbcSink.sink(
//                "insert into sensor values (?, ?, ?)",
                "insert into sensor(id, ts, vc) values(?,?,?) on duplicate key update id = values(id),ts=values(ts), vc=values(vc)",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement pst, WaterSensor element) throws SQLException {
                        pst.setString(1, element.getId());
                        pst.setLong(2, element.getTs());
                        pst.setInt(3, element.getVc());
                    }
                },
                new JdbcExecutionOptions.Builder().withBatchSize(3) // 执行写入的批次大小，默认5000条
                        .withBatchIntervalMs(500000L) // 执行写入的时间间隔，默认0，两个条件触发一个就执行写入
                        .build(),

                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test")
                        .withUsername("root")
                        .withPassword("123456")
                        .withDriverName(DriverManager.class.getName())
                        .build()

//                new JdbcOptions.Builder() // 报错：No tableName supplied.
//                        .setDBUrl("jdbc:mysql://hadoop102:3306/test")
//                        .setTableName("sensor") // Caused by: java.lang.ClassNotFoundException: org.apache.flink.table.api.ValidationException
//                        .setUsername("root")
//                        .setPassword("123456")
//                        .setDriverName(DriverManager.class.getName()) // 驱动类全类名 com.mysql.jdbc.Driver
//                        .build()
        ));

        env.execute();
    }
}
