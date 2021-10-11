package com.atguigu.preview.tableapi;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @description:
 * @create_time: 21:05 2021/8/11
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Sink_File02 {
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

        Schema schema = new Schema().field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        tableEnv.connect(new FileSystem().path("out/sensor.txt"))
                .withFormat(new Csv().fieldDelimiter('|'))
                .withSchema(schema)
                .createTemporaryTable("sensor");

        // 写出到文件，因为文件不支持撤回，所以不能写出聚合后的结果
/*        tableEnv.fromDataStream(source)
                .groupBy($("id"))
                .select($("id"), $("vc").sum().as("sum_vc"))
                .executeInsert("sensor");*/

        tableEnv.fromDataStream(source)
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"))
                .executeInsert("sensor");

//        env.execute();
    }
}
