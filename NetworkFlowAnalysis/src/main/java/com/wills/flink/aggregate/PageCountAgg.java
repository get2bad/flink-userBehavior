package com.wills.flink.aggregate;

import com.wills.flink.entity.ApacheLogEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author 王帅
 * @date 2021-02-22 15:48:09
 * @description:
 */
public class PageCountAgg implements AggregateFunction<ApacheLogEvent,Long,Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(ApacheLogEvent apacheLogEvent, Long aLong) {
        return aLong + 1;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return acc1 + aLong;
    }
}
