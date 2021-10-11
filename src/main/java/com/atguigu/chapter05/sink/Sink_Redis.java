package com.atguigu.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Arrays;
import java.util.List;

/**
 * @description:
 * @create_time: 13:37 2021/8/9
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Sink_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("sensor_1", 1547718199L, 35),
                new WaterSensor("sensor_6", 1547718201L, 15),
                new WaterSensor("sensor_7", 1547718202L, 6),
                new WaterSensor("sensor_10", 1547718205L, 38)
        );

        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .setDatabase(0)
                .build();

        env.fromCollection(waterSensors)
                .addSink(new RedisSink<>(jedisPoolConfig, new RedisMapper<WaterSensor>() {
                    @Override
                    public RedisCommandDescription getCommandDescription() {
                        return new RedisCommandDescription(RedisCommand.HSET, "sensor");
                    }

                    @Override
                    public String getKeyFromData(WaterSensor data) {
                        return data.getId();
                    }

                    @Override
                    public String getValueFromData(WaterSensor data) {
                        return JSON.toJSONString(data);
                    }
                }));


        env.execute();
    }
}
