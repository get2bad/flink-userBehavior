package com.wills.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 王帅
 * @date 2021-03-01 11:41:40
 * @description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MarketingUserBehavior {

    private Long userId;
    private String behavior;
    private String channel;
    private Long timestamp;
}
