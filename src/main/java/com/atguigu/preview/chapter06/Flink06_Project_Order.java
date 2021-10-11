package com.atguigu.preview.chapter06;

import com.atguigu.preview.chapter06.bean.OrderEvent;
import com.atguigu.preview.chapter06.bean.TxEvent;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @description:
 * @create_time: 11:07 2021/8/8
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink06_Project_Order {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 1. 读取Order流
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env
                .readTextFile("src/main/resources/OrderLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new OrderEvent(
                            Long.valueOf(datas[0]),
                            datas[1],
                            datas[2],
                            Long.valueOf(datas[3]));

                });
        // 2. 读取交易流
        SingleOutputStreamOperator<TxEvent> txDS = env
                .readTextFile("src/main/resources/ReceiptLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new TxEvent(datas[0], datas[1], Long.valueOf(datas[2]));
                });

        // 3. 两个流连接在一起
        ConnectedStreams<OrderEvent, TxEvent> orderAndTx = orderEventDS.connect(txDS);

        // 两条流分别分组
        orderAndTx.keyBy(OrderEvent::getTxId, TxEvent::getTxId)
                .process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
                    // 存 txId -> OrderEvent
                    Map<String, OrderEvent> orderMap = new HashMap<>();
                    // 存储 txId -> TxEvent
                    Map<String, TxEvent> txMap = new HashMap<>();

                    @Override
                    public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                        // 去对方的缓存查
                        if (txMap.containsKey(value.getTxId())) {
                            out.collect(value.getTxId() + "对账成功");
                        } else {
                            // 对账不成功，把自己加入缓存
                            orderMap.put(value.getTxId(), value);
                        }
                    }

                    @Override
                    public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                        if (orderMap.containsKey(value.getTxId())) {
                            out.collect(value.getTxId() + "对账成功");
                        } else {
                            txMap.put(value.getTxId(), value);
                        }
                    }
                }).print();

        env.execute();
    }
    
}
