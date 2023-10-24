package com.atguigu.app.day08;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_State_AggregatingState_Demo {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        // TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        // TODO 4.按照ID进行分组 计算每种传感器的平均水位
        SingleOutputStreamOperator<WaterSensor> mapDS = waterSensorDS.keyBy(WaterSensor::getId).map(new RichMapFunction<WaterSensor, WaterSensor>() {

            private AggregatingState<WaterSensor, Double> aggregatingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<WaterSensor, Tuple2<Double, Integer>, Double>("avg", new AggregateFunction<WaterSensor, Tuple2<Double, Integer>, Double>() {
                    @Override
                    public Tuple2<Double, Integer> createAccumulator() {
                        return new Tuple2<>(0.0D, 0);
                    }

                    @Override
                    public Tuple2<Double, Integer> add(WaterSensor value, Tuple2<Double, Integer> accumulator) {
                        accumulator.f0 = accumulator.f0 + value.getVc();
                        accumulator.f1 = accumulator.f1 + 1;
                        return accumulator;
                    }

                    @Override
                    public Double getResult(Tuple2<Double, Integer> accumulator) {
                        return accumulator.f0 / accumulator.f1;
                    }

                    @Override
                    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                        a.f0 = a.f0 + b.f0;
                        a.f1 = a.f1 + b.f1;
                        return a;
                    }
                }, Types.TUPLE(Types.DOUBLE, Types.INT)));
            }

            @Override
            public WaterSensor map(WaterSensor value) throws Exception {

                aggregatingState.add(value);
                value.setVc(aggregatingState.get());
                return value;
            }
        });

        // TODO 5.打印数据
        mapDS.print();

        // TODO 6.启动任务
        env.execute();
    }
}
