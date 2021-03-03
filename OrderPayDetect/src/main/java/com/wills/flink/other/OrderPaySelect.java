package com.wills.flink.other;

import com.wills.flink.entity.OrderEvent;
import com.wills.flink.entity.OrderResult;
import org.apache.flink.cep.PatternSelectFunction;

import java.util.List;
import java.util.Map;

/**
 * @author 王帅
 * @date 2021-03-03 14:06:31
 * @description:
 */
public class OrderPaySelect implements PatternSelectFunction<OrderEvent, OrderResult> {


    @Override
    public OrderResult select(Map<String, List<OrderEvent>> pattern) throws Exception {
        Long payedOrderId = pattern.get("pay").iterator().next().getOrderId();
        return new OrderResult(payedOrderId, "payed");
    }
}
