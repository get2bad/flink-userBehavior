package com.wills.flink.process;

import com.wills.flink.entity.LoginEvent;
import com.wills.flink.entity.LoginFailWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author 王帅
 * @date 2021-03-02 11:01:34
 * @description:
 */
public class LoginFailDetectWarning extends
        KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {
    private Integer failUpperBound;

    public LoginFailDetectWarning(Integer failUpperBound) {
        this.failUpperBound = failUpperBound;
    }

    // 定义状态
    ListState<LoginEvent> loginFailListState;
    ValueState<Long> timerTsState;

    @Override
    public void open(Configuration parameters) throws Exception {
        loginFailListState = getRuntimeContext().getListState(new
                ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class));
        timerTsState = getRuntimeContext().getState(new
                ValueStateDescriptor<Long>("timer-ts", Long.class));
    }

    @Override
    public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
        if ("fail".equals(value.getLoginState())) {
            Iterator<LoginEvent> iterator = loginFailListState.get().iterator();
            if (iterator.hasNext()) {
                LoginEvent firstFail = iterator.next();
                // 如果两次失败时间间隔小于 2 秒，输出报警
                if (value.getTimestamp() - firstFail.getTimestamp() <= 2) {
                    out.collect(new LoginFailWarning(value.getUserId(),
                            firstFail.getTimestamp(),
                            value.getTimestamp(),
                            "login fail in 2s"));
                }
                loginFailListState.clear();
                loginFailListState.add(value);
            } else {
                loginFailListState.add(value);
            }
        } else
            loginFailListState.clear();
    }
}
