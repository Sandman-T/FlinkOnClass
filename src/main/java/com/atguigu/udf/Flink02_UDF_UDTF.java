package com.atguigu.udf;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @description: 自定义炸裂函数：把一个字符串根据下划线“_”拆分，输出多行2列
 * @create_time: 11:08 2021/8/18
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink02_UDF_UDTF {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("senso", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_", 5000L, 50),
                new WaterSensor("sr_2", 6000L, 60));

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.将流转为动态表
        Table table = tableEnv.fromDataStream(waterSensorDataStreamSource);


        //TODO a.不注册直接使用
//        table.joinLateral(call(MySplit.class, $("id")))
//                .select($("id"), $("word"), $("length"))
//                .execute()
//                .print();

        //TODO b.先注册再使用
        tableEnv.createTemporarySystemFunction("myFun", MySplit.class);
        //Table API
//        table.joinLateral(call("myFun", $("id"))) // 这种不能取别名
//                .select($("id"), $("word"), $("length"))
//                .execute().print();

        // 换种写法
//        table.joinLateral("myFun(id) as (newword, newlength)")
//                .select($("id"), $("newword"), $("newlength"))
//                .execute().print();
        //SQL
        tableEnv.createTemporaryView("sensor", table);
        // join写法必须在最后加上on ture
//        tableEnv.executeSql("select id,word,length from sensor INNER JOIN LATERAL TABLE(myFun(id)) ON TRUE" ).print();
        tableEnv.executeSql("select id,word,length from sensor, LATERAL TABLE(myFun(id))" ).print();
    }

    @FunctionHint(output = @DataTypeHint("ROW(word String,length Integer)"))
    public static class MySplit extends TableFunction<Row> {
        public void eval(String str) {
            String[] strings = str.split("_");
            for (String s : strings) {
                collect(Row.of(s, s.length()));
            }
        }
    }
}
