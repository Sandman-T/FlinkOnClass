package com.atguigu.chapter06;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @description:
 * @create_time: 9:05 2021/8/10
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class FLink02_PV_Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.读取数据
        DataStreamSource<String> fileDS = env.readTextFile("src/main/resources/UserBehavior.csv");

        // 2.转样例类
        SingleOutputStreamOperator<UserBehavior> behaviorDS = fileDS.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(Long.parseLong(fields[0]),
                    Long.parseLong(fields[1]),
                    Integer.parseInt(fields[2]),
                    fields[3],
                    Long.parseLong(fields[4]));
        });

        // 使用process()一步完成
        behaviorDS.process(new ProcessFunction<UserBehavior, Tuple2<String, Long>>() {
            private Long count = 0L; // 知识和并行度为1的情况，如果多并行度，可以把count放在实现类的静态变量中
            @Override
            public void processElement(UserBehavior value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                // 过滤pv
                if ("pv".equals(value.getBehavior())) {
                    count++;
                    out.collect(Tuple2.of(value.getBehavior(), count));
                }
            }
        }).print();


        env.execute();
    }
}
