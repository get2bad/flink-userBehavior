package com.wills.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 王帅
 * @date 2021-03-01 17:50:07
 * @description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class BlackListWarning {

    private Long userId;
    private Long adId;
    private String warningMsg;
}
