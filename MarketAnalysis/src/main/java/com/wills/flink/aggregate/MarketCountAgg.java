package com.wills.flink.aggregate;

import com.wills.flink.entity.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author 王帅
 * @date 2021-03-01 11:51:42
 * @description:
 */
public class MarketCountAgg implements AggregateFunction<MarketingUserBehavior,Long,Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(MarketingUserBehavior marketingUserBehavior, Long aLong) {
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
