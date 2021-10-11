package com.atguigu.chapter07.state;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @description: 计算每个传感器的水位和
 * @create_time: 9:26 2021/8/13
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink03_KeyedState_Reducing {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> source = env.socketTextStream("hadoop102", 9527)
                .map(line -> {
                    String[] fields = line.split(" ");
                    return new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2]));
                });

        source.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    // 1.声明状态
                    private ReducingState<Integer> vcSumState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 2.初始化状态
                        vcSumState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>(
                                "sum_vc",
                                new ReduceFunction<Integer>() {
                                    @Override
                                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                                        return value1 + value2;
                                    }
                                },
                                Integer.class
                        ));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        // 把最新的数据添加到状态中
                        vcSumState.add(value.getVc());

                        // 3.获取状态
                        Integer vcSum = vcSumState.get();

                        // 4.更新状态

                        // 输出
                        out.collect("key:" + ctx.getCurrentKey() + "=>" + vcSum);
                    }

                    @Override
                    public void close() throws Exception {
                        // 5.清空状态
                        vcSumState.clear();
                    }

                }).print();

        env.execute();
    }
}
