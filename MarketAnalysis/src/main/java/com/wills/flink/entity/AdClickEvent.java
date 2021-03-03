package com.wills.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 王帅
 * @date 2021-03-01 15:51:03
 * @description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AdClickEvent {

    private Long userId;
    private Long adId;
    private String province;
    private String city;
    private Long timestamp;
}
