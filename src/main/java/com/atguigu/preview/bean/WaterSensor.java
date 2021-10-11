package com.atguigu.preview.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @description: 水位传感器：用于接收水位数据
 * @create_time: 20:21 2021/8/5
 * @author: Sandman
 * @version: 1.0
 * @modified By:
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String id; // 传感器编号
    private Long ts; // 时间戳
    private Integer vc; // 水位
}
