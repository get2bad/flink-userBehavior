package com.wills.flink.cep;

import com.wills.flink.entity.LoginEvent;
import com.wills.flink.entity.LoginFailWarning;
import org.apache.flink.cep.PatternSelectFunction;

import java.util.List;
import java.util.Map;

/**
 * @author 王帅
 * @date 2021-03-02 17:42:28
 * @description:
 */
public class LoginFaildMathDetect implements PatternSelectFunction<LoginEvent, LoginFailWarning> {
    @Override
    public LoginFailWarning select(Map<String, List<LoginEvent>> pattern) throws Exception {
        LoginEvent firstFail = pattern.get("firstFail").iterator().next();
        LoginEvent secondFail =
                pattern.get("secondFail").iterator().next();
        return new LoginFailWarning(firstFail.getUserId(),
                firstFail.getTimestamp(), secondFail.getTimestamp(), "login fail!");

    }
}
