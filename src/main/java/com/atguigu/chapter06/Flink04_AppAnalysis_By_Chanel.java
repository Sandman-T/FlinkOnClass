package com.atguigu.chapter06;

import com.atguigu.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @description:
 * @create_time: 10:11 2021/8/10
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink04_AppAnalysis_By_Chanel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<MarketingUserBehavior> marketingUserBehaviorDS = env.addSource(new AppMarketingDataSource());

        SingleOutputStreamOperator<Tuple2<String, Long>> marketBehavior2OneDS = marketingUserBehaviorDS.flatMap(new FlatMapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
            @Override
            public void flatMap(MarketingUserBehavior value, Collector<Tuple2<String, Long>> out) throws Exception {
                out.collect(Tuple2.of(value.getChannel() + "_" + value.getBehavior(), 1L));
            }
        });

        marketBehavior2OneDS.keyBy(0).sum(1).print();

        env.execute();
    }


    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        private Boolean isRunning = true;
        private Random random;
        private List<String> channels;
        private List<String> behaviors;

        @Override
        public void open(Configuration parameters) throws Exception {
            random = new Random();
            channels = Arrays.asList("huawei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
            behaviors = Arrays.asList("download", "install", "update", "uninstall");
        }

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (isRunning) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis());
                ctx.collect(marketingUserBehavior);
                Thread.sleep(200);

            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
