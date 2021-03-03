package com.wills.flink.action;

import com.wills.flink.cep.LoginFaildMathDetect;
import com.wills.flink.entity.LoginEvent;
import com.wills.flink.entity.LoginFailWarning;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;

/**
 * @author 王帅
 * @date 2021-03-02 15:54:31
 * @description:
 */
public class LoginFailWithCep {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL url = LoginFaildDetect.class.getResource("/LoginLog.csv");

        SingleOutputStreamOperator<LoginEvent> loginEventStream = env.readTextFile(url.getPath()).map(data -> {
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

        // 定义匹配模式
        Pattern<LoginEvent, LoginEvent> resCEP = Pattern.<LoginEvent>begin("firstFail").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent) throws Exception {
                return "fail".equals(loginEvent.getLoginState());
            }
        })
                .next("secondFail").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.getLoginState());
                    }
                }).within(Time.seconds(2));

        // 在数据流中匹配出定义好的模式
        PatternStream<LoginEvent> resStream = CEP.pattern(loginEventStream.keyBy(LoginEvent::getUserId), resCEP);

        SingleOutputStreamOperator<LoginFailWarning> res = resStream.select(new LoginFaildMathDetect());

        res.print();

        env.execute("登陆失败检测 - CEP");
    }
}
