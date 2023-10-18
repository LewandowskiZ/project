package com.atguigu.function;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class StringToWaterSensor implements MapFunction<String, WaterSensor> {

    @Override
    public WaterSensor map(String value) throws Exception {
        String[] split = value.split(",");
        return new WaterSensor(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
    }
}