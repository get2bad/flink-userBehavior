package com.wills.flink.process;

import com.wills.flink.entity.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * @author 王帅
 * @date 2021-02-22 15:54:59
 * @description:
 */
public class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String> {

    private int pageSize;

    private MapState<String, Long> pageViewCountMapState;

    public TopNHotPages(int pageSize) {
        this.pageSize = pageSize;
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        if (timestamp == ctx.getCurrentKey() + 60 * 1000L) {
            pageViewCountMapState.clear();
            return;
        }

        List<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(pageViewCountMapState.entries().iterator());

        pageViewCounts.sort(new Comparator<Map.Entry<String, Long>>() {
            @Override
            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                if (o2.getValue() > o1.getValue()) {
                    return 1;
                } else if (o1.getValue() > o2.getValue()) {
                    return -1;
                } else {
                    return 0;
                }
            }
        });
        StringBuilder result = new StringBuilder();
        result.append("====================================\n");
        result.append("窗口结束时间: ").append(new Timestamp(timestamp - 1)).append("\n");
        for (int i = 0; i < Math.min(pageSize, pageViewCounts.size()); i++) {
            Map.Entry<String, Long> currentPageViewCount =
                    pageViewCounts.get(i);
            result.append("No").append(i + 1).append(":")
                    .append(" 页面 URL=")
                    .append(currentPageViewCount.getKey())
                    .append(" 浏览量=")
                    .append(currentPageViewCount.getValue())
                    .append("\n");
        }
        result.append("====================================\n\n");
        Thread.sleep(1000);
        out.collect(result.toString());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        pageViewCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("page-count-map", String.class, Long.class));
    }

    @Override
    public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {
        pageViewCountMapState.put(value.getUrl(), value.getCount());

        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);

        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60 * 1000L);
    }
}
