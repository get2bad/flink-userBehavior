package com.wills.flink.window;

import com.wills.flink.entity.ItemViewCount;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author 王帅
 * @date 2021-02-22 14:36:24
 * @description:
 * 窗口方法： WindowFunction<IN, OUT, KEY, W extends Window>
 *                      IN: 输入参数  特指 聚合后的输出值
 *                      OUT: 输出参数
 *                      KEY:
 *                      W：被应用的time Window
 */
public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
        long itemId = tuple.getField(0);
        long windowEnd = window.getEnd();
        Long count = input.iterator().next();
        out.collect(new ItemViewCount(itemId,windowEnd,count));
    }
}
