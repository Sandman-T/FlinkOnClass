package com.atguigu.preview.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.ArrayList;


/**
 * @description: 把数据写入Kafka
 * @create_time: 22:17 2021/8/5
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink02_Sink_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .setDatabase(0)
                .setMaxTotal(100)
                .setTimeout(1000 * 10)
                .build();

        env.fromCollection(waterSensors)
                .addSink(new RedisSink<WaterSensor>(jedisPoolConfig, new MyRedisMapper()));

        env.execute();

    }

    public static class MyRedisMapper implements RedisMapper<WaterSensor> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET);
        }

        @Override
        public String getKeyFromData(WaterSensor data) {
            return data.getId();
        }

        @Override
        public String getValueFromData(WaterSensor data) {
            return JSON.toJSONString(data);
        }
    }
}
