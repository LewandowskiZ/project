package com.atguigu.app.day03;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Flink10_Transform_Connect {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取端口数据
        DataStreamSource<String> socketTextStream1 = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> socketTextStream2 = env.socketTextStream("hadoop102", 8888);

        // TODO 3.将socketTextStream1转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream1.map(new StringToWaterSensor());

        // TODO 4.连接两个流 打印数据
        waterSensorDS.connect(socketTextStream2).map(new CoMapFunction<WaterSensor, String, String>() {
            @Override
            public String map1(WaterSensor waterSensor) throws Exception {
                return waterSensor.toString();
            }

            @Override
            public String map2(String s) throws Exception {
                return s;
            }
        }).print();

        // TODO 5.启动任务
        env.execute();
    }
}
