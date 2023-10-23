package com.atguigu.app.day08;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

public class Flink02_State_ReducingState_Demo {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        // TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        // TODO 4.按照ID进行分组 计算每种传感器的水位和
        SingleOutputStreamOperator<WaterSensor> mapDS = waterSensorDS.keyBy(WaterSensor::getId).map(new RichMapFunction<WaterSensor, WaterSensor>() {

            private ReducingState<Double> reducingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Double>("sum", new ReduceFunction<Double>() {
                    @Override
                    public Double reduce(Double value1, Double value2) throws Exception {
                        return value1 + value2;
                    }
                }, Double.class));
            }

            @Override
            public WaterSensor map(WaterSensor value) throws Exception {

                reducingState.add(value.getVc());

                value.setVc(reducingState.get());

                return value;
            }
        });

        // TODO 5.打印数据
        mapDS.print();

        // TODO 6.启动任务
        env.execute();
    }
}
