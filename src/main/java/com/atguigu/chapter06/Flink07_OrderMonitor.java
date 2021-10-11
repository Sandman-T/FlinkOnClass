package com.atguigu.chapter06;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @description: 实时对账
 * @create_time: 11:28 2021/8/10
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink07_OrderMonitor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取Order数据
        SingleOutputStreamOperator<OrderEvent> orderDS = env.readTextFile("src/main/resources/OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new OrderEvent(Long.parseLong(fields[0]), fields[1], fields[2], Long.parseLong(fields[3]));
                    }
                });

        //  读取支付数据
        SingleOutputStreamOperator<TxEvent> txEventDS = env.readTextFile("src/main/resources/ReceiptLog.csv")
                .map(new MapFunction<String, TxEvent>() {
                    @Override
                    public TxEvent map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new TxEvent(fields[0], fields[1], Long.parseLong(fields[2]));
                    }
                });

        // 连接两条流
        (orderDS.connect(txEventDS)
                .keyBy("txId", "txId")
                .process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
                    private HashMap<String, OrderEvent> orderMap;
                    private HashMap<String, TxEvent> txMap;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        orderMap = new HashMap<String, OrderEvent>();
                        txMap = new HashMap<String, TxEvent>();
                    }

                    // 订单表：要去查支付表的缓存数据
                    @Override
                    public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                        if (txMap.containsKey(value.getTxId())) {
                            out.collect("订单：" + value.getOrderId() + "对账成功");
                            // 对账成功，就要把缓存清掉
                            txMap.remove(value.getTxId());
                        } else {
                            // 找不到，则把自己添加到缓存
                            orderMap.put(value.getTxId(), value);
                        }
                    }

                    // 支付表：要去查order表的缓存数据
                    @Override
                    public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                        if (orderMap.containsKey(value.getTxId())) {
                            out.collect("订单：" + orderMap.get(value.getTxId()) + "对账成功");
                            // 对账成功，就要把缓存清掉
                            orderMap.remove(value.getTxId());
                        } else {
                            // 找不到，则把自己添加到缓存
                            txMap.put(value.getTxId(), value);
                        }
                    }
                })).print();

        env.execute();
    }
}
