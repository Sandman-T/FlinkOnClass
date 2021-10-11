package com.atguigu.udf;

import com.atguigu.bean.WaterSensor;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;


/**
 * @description: 自定义聚合函数：求平均值
 * @create_time: 11:08 2021/8/18
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink03_UDF_UDAF {
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
//        table.groupBy($("id"))
//                .aggregate(call(MyAvg.class, $("vc")).as("avgvc"))
//                .select($("id"), $("avgvc"))
//                .execute()
//                .print();

        //TODO b.先注册再使用
        tableEnv.createTemporarySystemFunction("myFun", MyAvg.class);
        //Table API
//        table.groupBy($("id"))
//                .aggregate(call("myFun", $("vc")).as("avgvc"))
//                .select($("id"), $("avgvc"))
//                .execute()
//                .print();

        //SQL
        tableEnv.createTemporaryView("sensor", table);
        tableEnv.executeSql("select id,myFun(vc) from sensor group by id").print();

    }

    @Data
    @AllArgsConstructor
    public static class MyAcc{
        private Integer sum;
        private Integer count;
    }
    public static class MyAvg extends AggregateFunction<Double, MyAcc> {

        @Override
        public Double getValue(MyAcc accumulator) {
            return accumulator.sum * 1.0 / accumulator.count;
        }

        @Override
        public MyAcc createAccumulator() {
            return new MyAcc(0, 0);
        }

        public void accumulate(MyAcc accumulator, Integer vc) {
            accumulator.sum += vc;
            accumulator.count += 1;
        }
    }
}
