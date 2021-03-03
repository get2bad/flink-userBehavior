package com.wills.flink.window;

import com.wills.flink.entity.AdCountViewByProvince;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author 王帅
 * @date 2021-03-01 15:59:49
 * @description:
 */
public class AdCountResult implements WindowFunction<Long, AdCountViewByProvince,String, TimeWindow> {


    @Override
    public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<AdCountViewByProvince> out) throws Exception {
        String windowEnd = new Timestamp(window.getEnd()).toString();
        Long count = input.iterator().next();
        out.collect( new AdCountViewByProvince(s, windowEnd, count) );
    }
}
