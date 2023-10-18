package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensorWithWindowTime {
    private String id;
    private String stt;
    private String edt;
    private Double vc;
    private Integer flag;
}
