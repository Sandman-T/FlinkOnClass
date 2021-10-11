package com.atguigu.chapter07.state;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @description: 同一传感器检测到连续两次水位差>10则报警
 * @create_time: 9:26 2021/8/13
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink01_KeyedState_Value {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> source = env.socketTextStream("hadoop102", 9527)
                .map(line -> {
                    String[] fields = line.split(" ");
                    return new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2]));
                });

        SingleOutputStreamOperator<String> result = source.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    // 1.声明状态
                    private ValueState<Integer> lastVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("每个并行度执行一次");
                        // 2.初始化状态
                        lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVc", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        // 3.获取状态
                        // 注意：当一条数据进来的时候，如果没有给初始值，会报空指针
                        Integer lastVc = lastVcState.value() == null ? value.getVc() : lastVcState.value();
                        if (Math.abs(value.getVc() - lastVc) > 10) {
                            ctx.output(new OutputTag<String>("报警") {
                            }, "水位差超过10，报警");
                        }

                        // 4.更新状态
                        lastVcState.update(value.getVc());

                        out.collect(value.toString());
                    }

                    @Override
                    public void close() throws Exception {
                        // 5.清空状态
                        lastVcState.clear();
                    }
                });

        result.print("主流");

        result.getSideOutput(new OutputTag<String>("报警"){}).print("报警");

        env.execute();
    }
}
