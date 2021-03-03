package com.wills.flink.action;

import com.wills.flink.aggregate.AdCountAgg;
import com.wills.flink.aggregate.MarketCountAgg;
import com.wills.flink.entity.*;
import com.wills.flink.process.FilterBlackListUser;
import com.wills.flink.source.SimulatedMarketingBehaviorSource;
import com.wills.flink.window.AdCountResult;
import com.wills.flink.window.MarketingCountResult;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * @author 王帅
 * @date 2021-03-01 15:50:31
 * @description:
 */
public class AdStatisticsByProvince {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL url = AdStatisticsByProvince.class.getResource("/AdClickLog.csv");

        SingleOutputStreamOperator<AdClickEvent> adClickEventStream = env.readTextFile(url.getPath()).map(data -> {
            String[] fields = data.split(",");
            return new AdClickEvent(new Long(fields[0]),
                    Long.valueOf(fields[1]), fields[2], fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {

            @Override
            public long extractAscendingTimestamp(AdClickEvent element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 自定义过程函数，进行过滤
        SingleOutputStreamOperator<AdClickEvent> filteredAdClickStream = adClickEventStream
                .keyBy("userId", "adId")
                .process(new FilterBlackListUser(100));

        // 根据 province 分组开窗聚合
        DataStream<AdCountViewByProvince> adCountDataStream = filteredAdClickStream
                .keyBy(AdClickEvent::getProvince)
                .timeWindow(Time.hours(1), Time.seconds(5) )
                .aggregate( new AdCountAgg(), new AdCountResult() );

        adCountDataStream.print();
        filteredAdClickStream.getSideOutput(new OutputTag<BlackListWarning>("blackList"){}).print("blackList:");


        env.execute();
    }
}
