package com.wills.flink.action;

import com.wills.flink.aggregate.MarketCountAgg;
import com.wills.flink.entity.ChannelPromotionCount;
import com.wills.flink.entity.MarketingUserBehavior;
import com.wills.flink.source.SimulatedMarketingBehaviorSource;
import com.wills.flink.window.MarketingCountResult;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author 王帅
 * @date 2021-03-01 11:47:15
 * @description:
 *
 */
public class AppMarketingByChannel {

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
                .keyBy("channel", "behavior")
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new MarketCountAgg(), new MarketingCountResult());

        resultStream.print();


        env.execute();
    }
}
