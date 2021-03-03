package com.wills.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 王帅
 * @date 2021-03-01 15:57:36
 * @description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AdCountViewByProvince {

    private String province;
    private String windowEnd;
    private Long count;
}
