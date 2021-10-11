package com.atguigu.preview.udf;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @description:
 * @create_time: 5:55 2021/8/13
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class UDF_Scalar {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> source = env.socketTextStream("hadoop102", 9527)
                .map(line -> {
                    String[] fields = line.split(" ");
                    return new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2]));
                });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.createTemporaryView("sensor", source);

        // 未注册直接使用
//        tableEnv.from("sensor")
//                .select(call(MySubstr.class, $("id"), 2, 5).as("substr"), $("id"))
//                .execute()
//                .print();


        // 先注册再使用
        tableEnv.createTemporarySystemFunction("mystr", MySubstr.class);
        tableEnv.executeSql("select mystr(id, 2, 5), id, ts, vc from sensor")
                .print();

    }


    public static class MySubstr extends ScalarFunction {

        public String eval(String str, Integer start, Integer end) {
            return str.substring(start, end);
        }

    }
}
