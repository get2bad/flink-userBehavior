package com.wills.flink.action;

import com.wills.flink.aggregate.MarketingStatisticsAgg;
import com.wills.flink.entity.ChannelPromotionCount;
import com.wills.flink.entity.MarketingUserBehavior;
import com.wills.flink.source.SimulatedMarketingBehaviorSource;
import com.wills.flink.window.MarketingStatisticsResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author 王帅
 * @date 2021-03-01 15:08:19
 * @description:
 */
public class AppMarketingStatistics {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<MarketingUserBehavior> dataStream = env.addSource(new SimulatedMarketingBehaviorSource()).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketingUserBehavior>() {
            @Override
            public long extractAscendingTimestamp(MarketingUserBehavior element) {
                return element.getTimestamp();
            }
        });

        SingleOutputStreamOperator<ChannelPromotionCount> resultStream = dataStream
                .filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(MarketingUserBehavior marketingUserBehavior) throws Exception {
                        return new Tuple2<>("total", 1L);
                    }
                })
                .keyBy(0)
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new MarketingStatisticsAgg(), new MarketingStatisticsResult());

        resultStream.print();


        env.execute();
    }
}
