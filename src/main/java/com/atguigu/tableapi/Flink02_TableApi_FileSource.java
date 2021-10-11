package com.atguigu.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @description:
 * @create_time: 17:14 2021/8/17
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink02_TableApi_FileSource {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", "BIGINT")
                .field("vc", "INT");

        tableEnv.connect(new FileSystem().path("src/main/resources/sensor.txt"))
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
                .withSchema(schema)
                .createTemporaryTable("sensor");

        tableEnv.from("sensor")
                .select($("id"), $("ts"), $("vc"))
                .execute()
                .print();
    }
}
