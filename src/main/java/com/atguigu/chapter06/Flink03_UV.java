package com.atguigu.chapter06;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.expressions.In;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @description:
 * @create_time: 9:35 2021/8/10
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink03_UV {
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

        behaviorDS.filter(behavior -> "pv".equals(behavior.getBehavior()))
                .process(new ProcessFunction<UserBehavior, Integer>() {
                    private HashSet<Long> userIds;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        userIds = new HashSet<Long>();
                    }

                    @Override
                    public void processElement(UserBehavior value, Context ctx, Collector<Integer> out) throws Exception {
                        userIds.add(value.getUserId());
                        out.collect(userIds.size());
                    }
                }).print();

        env.execute();
    }
}
