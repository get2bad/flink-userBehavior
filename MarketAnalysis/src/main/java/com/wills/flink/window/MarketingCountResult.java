package com.wills.flink.window;

import com.wills.flink.entity.ChannelPromotionCount;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author 王帅
 * @date 2021-03-01 11:53:30
 * @description:
 */
public class MarketingCountResult extends ProcessWindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow> {


    @Override
    public void process(Tuple tuple, Context context, Iterable<Long> elements, Collector<ChannelPromotionCount> out) throws Exception {
        String channel = tuple.getField(0);
        String behavior = tuple.getField(1);
        String windowEnd = new
                Timestamp( context.window().getEnd() ).toString();
        Long count = elements.iterator().next();
        out.collect( new ChannelPromotionCount(channel, behavior,
                windowEnd, count) );
    }
}
