package com.atguigu.app.day09;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

public class FlinkTest01 {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        // TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("water-sensor") {
        };
        SingleOutputStreamOperator<WaterSensor> processDS = waterSensorDS.keyBy(WaterSensor::getId).process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

            private MapState<String, Integer> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {

                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>("water-sensor", String.class, Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {

                if (!mapState.contains(value.id)) {
                    mapState.put(value.id, 1);
                    out.collect(value);
                } else {
                    ctx.output(outputTag, value);
                }
            }
        });

        processDS.print("Main>>>>>>");
        processDS.getSideOutput(outputTag).print("Side>>>>>>");

        env.execute();
    }
}
