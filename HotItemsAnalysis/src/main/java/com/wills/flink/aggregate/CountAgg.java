package com.wills.flink.aggregate;

import com.wills.flink.entity.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author 王帅
 * @date 2021-02-22 14:36:08
 * @description:
 * AggregateFunction<IN, ACC, OUT>:
 * 聚合接口， IN 输入的对象
 *          ACC 计算的数据类型
 *          OUT 输出的类型
 */
public class CountAgg implements AggregateFunction<UserBehavior,Long,Long> {


    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior userBehavior, Long aLong) {
        return aLong + 1L;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong + acc1;
    }
}
