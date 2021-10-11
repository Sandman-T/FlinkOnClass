package com.atguigu.tableapi;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @description:
 * @create_time: 19:07 2021/8/17
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink06_SQL_BaseUse {
    public static void main(String[] args) throws Exception {
        // 创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取输入流数据
        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        );

        // 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 把输入流转换成表
        Table table = tableEnv.fromDataStream(source);
        // 也可以直接把流注册成一张临时表
//        tableEnv.createTemporaryView("watersensor", source);

        // 使用sql查询未注册的表
        tableEnv.sqlQuery("select * from " + table + " where id = 'sensor_1'")
                .execute()
                .print();


        // 把table对象注册成临时表
        tableEnv.createTemporaryView("watersensor", table);
        // 使用sql查询一个已注册的表
        Table sqlQuery = tableEnv.sqlQuery("select id,sum(vc) from watersensor group by id");
        tableEnv.toRetractStream(sqlQuery, Row.class)
                .print();

        env.execute();

    }
}
