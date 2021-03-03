package com.wills.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 王帅
 * @date 2021-03-01 11:46:20
 * @description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ChannelPromotionCount {

    private String channel;
    private String behavior;
    private String windowEnd;
    private Long count;
}
