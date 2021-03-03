package com.wills.flink.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author 王帅
 * @date 2021-03-01 15:12:07
 * @description:
 */
public class MarketingStatisticsAgg implements AggregateFunction<Tuple2<String, Long>,Long,Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Tuple2<String, Long> stringLongTuple2, Long aLong) {
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
