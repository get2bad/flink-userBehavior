package com.wills.flink.aggregate;

import com.wills.flink.entity.AdClickEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author 王帅
 * @date 2021-03-01 15:58:27
 * @description:
 */
public class AdCountAgg implements AggregateFunction<AdClickEvent,Long,Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(AdClickEvent adClickEvent, Long aLong) {
        return aLong + 1L;
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
