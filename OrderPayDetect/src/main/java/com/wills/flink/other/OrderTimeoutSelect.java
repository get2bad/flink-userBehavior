package com.wills.flink.other;

import com.wills.flink.entity.OrderEvent;
import com.wills.flink.entity.OrderResult;
import org.apache.flink.cep.PatternTimeoutFunction;

import java.util.List;
import java.util.Map;

/**
 * @author 王帅
 * @date 2021-03-03 14:06:21
 * @description:
 */
public class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent, OrderResult> {
    @Override
    public OrderResult timeout(Map<String, List<OrderEvent>> pattern, long l) throws Exception {
        Long timeoutOrderId = pattern.get("create").iterator().next().getOrderId();
        return new OrderResult(timeoutOrderId, "timeout " + l);
    }
}
