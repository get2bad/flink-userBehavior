package com.wills.flink.process;

import com.wills.flink.entity.AdClickEvent;
import com.wills.flink.entity.BlackListWarning;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author 王帅
 * @date 2021-03-01 17:47:28
 * @description:
 */
public class FilterBlackListUser extends ProcessFunction<AdClickEvent, AdClickEvent> {

    private Integer time;

    // 自定义状态
    ValueState<Long> countState;
    ValueState<Boolean> isSentState;

    public FilterBlackListUser(Integer time) {
        this.time = time;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ad-count",Long.class,0L));
        isSentState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-sent-count",Boolean.class,false));
    }

    @Override
    public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
        Long count = countState.value();
        if(count == 0){
            // 如果是第一次处理，那么就注册一个定时器 定时器的时间为1天，1天后自动清除
            Long ts = (ctx.timerService().currentProcessingTime() /
                    (24*60*60*1000) + 1) * (24*60*60*1000);
            ctx.timerService().registerEventTimeTimer(ts);
        }

        // 如果计数达到上限，就加入黑名单
        if(count >= time){
            if(!isSentState.value()){
                isSentState.update(true);
                ctx.output(new OutputTag<BlackListWarning>("blackList"){},
                        new BlackListWarning(value.getUserId(),value.getAdId(),"点击超过" + time + "次！"));
            }
            return;
        }
        countState.update(count + 1);
        out.collect(value);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
        // 触发定时器 1天后清除所有的count 还有状态
        countState.clear();
        isSentState.clear();
    }
}
