package com.atguigu.preview.udf;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @description:
 * @create_time: 20:47 2021/8/10
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink01_UDF_Scalar {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取文件得到DataStream
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        //3.将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDataStreamSource);

        // 先注册再使用
        tableEnv.createTemporarySystemFunction("myLength", MyLength.class);
//        table.select(call("myLength", $("id"))).execute().print();

        // 不注册直接用
//        table.select(call(MyLength.class, $("id"))).execute().print();

        // sql
        tableEnv.executeSql("select myLength(id) from " + table).print();


// Exception in thread "main" java.lang.IllegalStateException: No operators defined in streaming topology. Cannot execute.
//        env.execute();
    }


    public static class MyLength extends ScalarFunction {

        public Integer eval (String str) {
            return str.length();
        }

    }
}
