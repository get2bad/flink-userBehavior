package com.wills.flink.window;

import com.wills.flink.entity.PageViewCount;
import com.wills.flink.entity.UserBehavior;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * @author 王帅
 * @date 2021-02-24 11:52:48
 * @description:
 */
public class UvCountResult implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {

    @Override
    public void apply(TimeWindow window, java.lang.Iterable<UserBehavior> values, Collector<PageViewCount> out) throws Exception {
        Set<Long> idSet = new HashSet<>();
        for( UserBehavior ub: values ){
            idSet.add(ub.getUserId());
        }
        out.collect( new PageViewCount("uv", window.getEnd(),
                (long)idSet.size()) );
    }
}
