package com.atguigu.chapter07.state;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @description: 计算每个传感器的平均水位
 * @create_time: 9:26 2021/8/13
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink04_KeyedState_Agg {
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
                    private AggregatingState<Integer, Double> vcAvgState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 2.初始化状态
                        vcAvgState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>(
                                "vcAvg",
                                new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                    @Override
                                    public Tuple2<Integer, Integer> createAccumulator() {
                                        return Tuple2.of(0, 0);
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                        return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
                                    }

                                    @Override
                                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                        return accumulator.f0 * 1.0 / accumulator.f1;
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                                    }
                                },
                                Types.TUPLE(Types.INT, Types.INT)
                        ));
                    }


                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        // 把最新的数据添加到状态中
                        // 4.更新状态
                        vcAvgState.add(value.getVc());

                        // 3.获取状态
                        Double vcAvg = vcAvgState.get();

                        // 输出
                        out.collect("key:" + ctx.getCurrentKey() + "=>" + vcAvg);

                    }

                    @Override
                    public void close() throws Exception {
                        // 5.清空状态
                        vcAvgState.clear();
                    }

                }).print();

        env.execute();
    }
}
