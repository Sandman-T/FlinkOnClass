package com.atguigu.chapter05.source;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @description:
 * @create_time: 17:35 2021/8/7
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink05_Source_Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<WaterSensor> dataStreamSource = env.addSource(new MySource());

        dataStreamSource.print();

        env.execute();
    }

    private static class MySource implements SourceFunction<WaterSensor> {
        private Boolean isRunning = true;
        private Random random = new Random();


        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            while (isRunning) {
                ctx.collect(new WaterSensor("sensor_" + random.nextInt(10), System.currentTimeMillis(), random.nextInt(100)));
                Thread.sleep(30000L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
