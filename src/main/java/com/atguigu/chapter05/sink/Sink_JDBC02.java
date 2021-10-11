package com.atguigu.chapter05.sink;

import com.atguigu.bean.WaterSensor;
import com.mysql.jdbc.Driver;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description:
 * @create_time: 15:12 2021/8/12
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Sink_JDBC02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("sensor_1", 1547718199L, 35),
                new WaterSensor("sensor_6", 1547718201L, 15),
                new WaterSensor("sensor_7", 1547718202L, 6),
                new WaterSensor("sensor_10", 1547718205L, 38)
        );
        String sql = "insert into sensor(id, ts, vc) values(?,?,?) on duplicate key update id = values(id),ts=values(ts), vc=values(vc)";

        source.addSink(JdbcSink.sink(
                sql,
                (pst, element) -> {
                    pst.setString(1, element.getId());
                    pst.setLong(2, element.getTs());
                    pst.setInt(3, element.getVc());
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5) // 触发写入的数据条数
                        .withBatchIntervalMs(5000L) // 触发写入数据的间隔时间
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test")
                        .withDriverName(Driver.class.getName()) // com.mysql.jdbc.Driver
                        .withUsername("root")
                        .withPassword("root")
                        .build()
        ));

        env.execute();
    }
}
