package com.atguigu.chapter07.state;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @description: 去重: 去掉重复的水位值. 思路: 把水位值作为MapState的key来实现去重, value随意
 * @create_time: 9:26 2021/8/13
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink05_KeyedState_Map {
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
                    private MapState<Integer, WaterSensor> vcUniqueState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 2.初始化状态
                        vcUniqueState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, WaterSensor>(
                                "vcUniq",
                                Integer.class,
                                WaterSensor.class
                        ));
                    }


                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        // 把最新的数据添加到状态中
                        // 4.更新状态
                        if (!vcUniqueState.contains(value.getVc())) {
                            vcUniqueState.put(value.getVc(), value);
                        }

                        // 3.获取状态
                        WaterSensor waterSensor = vcUniqueState.get(value.getVc());

                        // 输出
                        out.collect(waterSensor.toString());

                    }

                    @Override
                    public void close() throws Exception {
                        // 5.清空状态
                        vcUniqueState.clear();
                    }

                }).print();

        env.execute();
    }
}
