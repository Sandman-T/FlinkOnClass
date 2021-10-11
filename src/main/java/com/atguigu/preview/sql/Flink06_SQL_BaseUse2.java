package com.atguigu.preview.sql;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @description: 使用sql查询已经注册的表
 * @create_time: 18:41 2021/8/10
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink06_SQL_BaseUse2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table inputTable = tableEnv.fromDataStream(source);
        // TODO 把Table对象注册成表
        tableEnv.createTemporaryView("sensor", inputTable);

        // 方式二：直接从流，转换成表，并注册表名(没有 Table的对象)
        tableEnv.createTemporaryView("sensor2", source);
        // 这种方式，如果需要 Table对象，可以从表名获取
        Table table = tableEnv.from("sensor_2");

        // TODO 注意from和where之间的空格，前后都要加
        Table sqlQuery = tableEnv.sqlQuery("select * from sensor2 where id='sensor_1'");
        tableEnv.toAppendStream(sqlQuery, Row.class).print();

        env.execute();
    }
}
