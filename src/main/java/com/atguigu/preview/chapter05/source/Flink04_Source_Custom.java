package com.atguigu.preview.chapter05.source;

import com.atguigu.preview.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 * @description: 自定义数据源
 * @create_time: 20:56 2021/8/5
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */
public class Flink04_Source_Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> mySourceDS = env.addSource(new MySource("hadoop102", 9527));
        mySourceDS.print("my source");
        env.execute();
    }

    public static class MySource implements SourceFunction<WaterSensor> {
        private String host;
        private Integer port;
        private Socket socket;
        private volatile boolean isRunning = true;

        public MySource() {}
        public MySource(String host, Integer port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            // Socket客户端
            Socket socket = new Socket(host, port);
            InputStream is = socket.getInputStream();
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line;
            while (isRunning && (line = br.readLine()) != null) {
                String[] strings = line.split(",");
                ctx.collect(new WaterSensor(strings[0], Long.valueOf(strings[1]), Integer.valueOf(strings[2])));
            }
            br.close();
            isr.close();
            is.close();
        }

        @Override
        public void cancel() {
            isRunning = false;
            // TODO 关闭资源

            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
