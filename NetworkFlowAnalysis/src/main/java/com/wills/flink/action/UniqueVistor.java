package com.wills.flink.action;

import com.wills.flink.entity.PageViewCount;
import com.wills.flink.entity.UserBehavior;
import com.wills.flink.window.UvCountResult;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;

/**
 * @author 王帅
 * @date 2021-02-24 11:49:58
 * @description:
 * 独立的访问用户数
 */
public class UniqueVistor {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        // 设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        URL resource = UniqueVistor.class.getResource("/UserBehavior.csv");
        DataStreamSource<String> source = env.readTextFile(resource.getPath());

        // 转换实体类
        // 3. 转换为POJO，分配时间戳和watermark
        DataStream<UserBehavior> dataStream = source
                .map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<PageViewCount> uniqueVistor = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .apply(new UvCountResult());

        uniqueVistor.print();

        env.execute("unique vistor");
    }
}
