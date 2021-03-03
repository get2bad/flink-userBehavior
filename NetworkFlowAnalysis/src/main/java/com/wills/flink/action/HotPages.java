package com.wills.flink.action;

import com.wills.flink.aggregate.PageCountAgg;
import com.wills.flink.entity.ApacheLogEvent;
import com.wills.flink.entity.PageViewCount;
import com.wills.flink.process.TopNHotPages;
import com.wills.flink.window.PageCountResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

/**
 * @author 王帅
 * @date 2021-02-22 15:41:17
 * @description:
 */
public class HotPages {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        // 设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        URL resource = HotPages.class.getResource("/apache.log");
        DataStreamSource<String> source = env.readTextFile(resource.getPath());

        // 转换为实体类
        SingleOutputStreamOperator<ApacheLogEvent> dataStream = source.map(line -> {
            String[] fields = line.split(" ");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
            return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5],
                    fields[6]);
            // 1分钟的 water mark 的延迟时间
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.minutes(1)) {
            @Override
            public long extractTimestamp(ApacheLogEvent element) {
                return element.getTimestamp();
            }
        });

        // 做聚合
        SingleOutputStreamOperator<PageViewCount> aggDStream = dataStream
                .filter(data -> "GET".equals(data.getMethod()))
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(new OutputTag<ApacheLogEvent>("late") {
                })
                .aggregate(new PageCountAgg(), new PageCountResult());

        // 聚合完成 进行key 收集同一窗口所有商品的count值，排序输出topN
        SingleOutputStreamOperator count = aggDStream.keyBy(PageViewCount::getWindowEnd).process(new TopNHotPages(5));

        count.print();

        aggDStream.getSideOutput(new OutputTag<ApacheLogEvent>("late") {
        }).print("late");


        env.execute("NetworkFlow");
    }
}
