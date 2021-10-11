package com.atguigu.chapter07.state;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @description: 针对每个传感器输出最高的3个水位值
 * @create_time: 9:26 2021/8/13
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink02_KeyedState_List {
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
                    private ListState<Integer> top3VCState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 2.初始化状态
                        top3VCState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("top3", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        // 把最新的数据添加到状态中
                        top3VCState.add(value.getVc());

                        // 3.获取状态
                        List<Integer> top3VCs = new ArrayList<>();
                        for (Integer vc : top3VCState.get()) {
                            top3VCs.add(vc);
                        }

                        // 排序，保留前3
                        top3VCs.sort((o1, o2) -> o2 - o1);
                        if (top3VCs.size() > 3) {
                            top3VCs.remove(3); // 长度最大只可能是4，所以remove 3就行了
                        }

                        // 4.更新状态
                        top3VCState.update(top3VCs);

                        // 输出
                        out.collect("key:" + ctx.getCurrentKey() + "=>" + top3VCs.toString());
                    }

                    @Override
                    public void close() throws Exception {
                        // 5.清空状态
                        top3VCState.clear();
                    }

                }).print();

        env.execute();
    }
}
