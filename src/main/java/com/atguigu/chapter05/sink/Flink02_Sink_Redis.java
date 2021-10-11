package com.atguigu.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;

/**
 * @description:
 * @create_time: 9:21 2021/8/8
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink02_Sink_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));



        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .setDatabase(0)
                .setMaxTotal(10)
                .setTimeout(2000)
                .setMaxIdle(3)
                .build();

        env.fromCollection(waterSensors)
                .addSink(new RedisSink<WaterSensor>(
                        flinkJedisPoolConfig,
                        new MyRedisMapper()
                ));

        env.execute();
    }

    public static class MyRedisMapper implements RedisMapper<WaterSensor> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            // hash 还需要第二个参数，是最外层的redis key
            new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature");
            return new RedisCommandDescription(RedisCommand.SET); // string
        }

        @Override
        public String getKeyFromData(WaterSensor data) {
            return data.getId(); // string等：redis key； hash、zset：内部的key
        }

        @Override
        public String getValueFromData(WaterSensor data) {
            return JSON.toJSONString(data);
        }
    }

}
