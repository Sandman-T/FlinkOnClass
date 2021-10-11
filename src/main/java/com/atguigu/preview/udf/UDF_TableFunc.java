package com.atguigu.preview.udf;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @description:
 * @create_time: 6:09 2021/8/13
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class UDF_TableFunc {
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

        tableEnv.from("sensor")
                .joinLateral(call(MySplit.class, $("id"), "_").as("newword", "newlength"))
                .select($("id"), $("ts"), $("vc"), $("newword"), $("newlength"))
                .execute()
                .print();

    }

    @FunctionHint(output = @DataTypeHint("Row<word String, length Integer>"))
    public static class MySplit extends TableFunction<Row> {
        public void eval(String str, String separator) {
            String[] strings = str.split(separator);
            for (String s : strings) {
                collect(Row.of(s, s.length()));
            }
        }
    }

}
