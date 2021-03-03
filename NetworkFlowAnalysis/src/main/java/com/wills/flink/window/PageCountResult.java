package com.wills.flink.window;

import com.wills.flink.entity.PageViewCount;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Tuple;

/**
 * @author 王帅
 * @date 2021-02-22 15:49:27
 * @description:
 */
public class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {
    @Override
    public void apply(String url, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
        out.collect(new PageViewCount(url,window.getEnd(),input.iterator().next()));
    }
}
