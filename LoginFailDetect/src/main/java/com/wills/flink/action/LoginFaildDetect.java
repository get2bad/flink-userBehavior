package com.wills.flink.action;

import com.wills.flink.entity.LoginEvent;
import com.wills.flink.entity.LoginFailWarning;
import com.wills.flink.process.LoginFailDetectWarning;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;

/**
 * @author 王帅
 * @date 2021-03-02 10:25:38
 * @description:
 */
public class LoginFaildDetect {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL url = LoginFaildDetect.class.getResource("/LoginLog.csv");

        SingleOutputStreamOperator<LoginEvent>  loginEventStream = env.readTextFile(url.getPath()).map(data -> {
            // 转换为实体类
            String[] fields = data.split(",");
            return new LoginEvent(new Long(fields[0]), fields[1],
                    fields[2], new Long(fields[3]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(LoginEvent element) {
                return element.getTimestamp() * 1000L;
            }
        });

        SingleOutputStreamOperator process = loginEventStream.keyBy(LoginEvent::getUserId)
                .process(new LoginFailDetectWarning(2));

        process.print("fail:");
        env.execute("登陆失败检测 - process");
    }
}
