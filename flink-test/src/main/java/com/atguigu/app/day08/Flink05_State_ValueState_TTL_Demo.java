package com.atguigu.app.day08;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink05_State_ValueState_TTL_Demo {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        // TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());


        OutputTag<String> outputTag = new OutputTag<String>("warn") {
        };
        SingleOutputStreamOperator<WaterSensor> processDS = waterSensorDS.keyBy(WaterSensor::getId).process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

            private ValueState<Double> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {

                ValueStateDescriptor<Double> valueStateDescriptor = new ValueStateDescriptor<>("warn", Double.class);
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.seconds(5))
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                        .build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                valueState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {

                Double value1 = valueState.value();

                if (value1 == null || Math.abs(value1 - value.getVc()) > 10) {
                    ctx.output(outputTag, value.getId() + "号传感器连续两次VC差值超过10!");
                } else {
                    out.collect(value);
                }
                valueState.update(value.getVc());
            }
        });

        processDS.print("Main>>>>>");
        processDS.getSideOutput(outputTag).print("Side>>>>>");

        env.execute();
    }
}
