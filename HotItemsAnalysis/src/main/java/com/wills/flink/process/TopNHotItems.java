package com.wills.flink.process;


import com.wills.flink.entity.ItemViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;

/**
 * @author 王帅
 * @date 2021-02-22 14:43:31
 * @description:
 */
public class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount,String> {

    private int topN;

    // 定义状态，所有ItemViewCount的 list
    private ListState<ItemViewCount> itemViewCountListState;

    public TopNHotItems(int topN) {
        this.topN = topN;
    }

    // 调用处理开始时调用本方法
    @Override
    public void open(Configuration parameters) throws Exception {
        // 拿到这个 状态，然后进行赋值，方便后续的处理操作
        itemViewCountListState = getRuntimeContext()
                .getListState(new ListStateDescriptor<ItemViewCount>("item-count-list", ItemViewCount.class));
    }

    @Override
    public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
        itemViewCountListState.add(value);
        // 注册一个 time + 1 s 的timer定时器
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);
    }

    // 定时器被处罚的时候的操作
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        List<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());

        // 进行 集合排序
        itemViewCounts.sort((o1,o2) -> o2.getCount().intValue() - o1.getCount().intValue());
        // 将排名信息格式化为 String
        StringBuilder sb = new StringBuilder();
        sb.append("====================================\r\n");
        sb.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\r\n");
        for (int i = 0; i < topN; i++) {
            ItemViewCount itemViewCount = itemViewCounts.get(i);
            sb.append("NO.").append(i+1).append(":").append("商品ID=").append(itemViewCount.getItemId())
                    .append("浏览量=").append(itemViewCount.getCount()).append("\r\n");
        }
        sb.append("====================================\r\n");

        // 控制输出频率，模拟实时滚动结果
        Thread.sleep(1000);
        out.collect(sb.toString());
    }
}
