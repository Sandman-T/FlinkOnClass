package com.atguigu.preview.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @description:
 * @create_time: 20:00 2021/8/10
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink07_SQL_Kafka2Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table source_sensor (id string, ts bigint, vc int)\n" +
                " with ('connector' = 'kafka',\n" +
                " 'topic' = 'flink_sql_source_sensor',\n" +
                " 'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                " 'properties.group.id' = 'atguigu',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'format' = 'json'\n" +
                " )");

        tableEnv.executeSql("create table sink_sensor (id string, ts bigint, vc int)\n" +
                " with ('connector' = 'kafka',\n" +
                " 'topic' = 'flink_sql_sink_sensor',\n" +
                " 'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                " 'format' = 'json'\n" +
                " )");

        tableEnv.executeSql("insert into sink_sensor select * from source_sensor where id='sensor_1' ");

//        env.execute();
    }
}
