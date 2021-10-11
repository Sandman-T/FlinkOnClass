package com.atguigu.chapter06;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @description:
 * @create_time: 9:05 2021/8/10
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class FLink01_PV {
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

        // 3.过滤pv
        SingleOutputStreamOperator<UserBehavior> pvBehaviorDS = behaviorDS.filter(behavior -> "pv".equals(behavior.getBehavior()));

        // 4.转换结构 -> (pv, 1)
        SingleOutputStreamOperator<Tuple2<String, Long>> pv2OneDS = pvBehaviorDS.map(behavior -> Tuple2.of(behavior.getBehavior(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 5.分组求和
        pv2OneDS.keyBy(0).sum(1).print();


        env.execute();
    }
}
