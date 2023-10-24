package com.atguigu.app.day09;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink01_State_BroadcastState {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取端口数据
        DataStreamSource<String> socketTextStream1 = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> socketTextStream2 = env.socketTextStream("hadoop102", 8888);

        // TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream1.map(new StringToWaterSensor());
        SingleOutputStreamOperator<WaterSensor> vcDS = socketTextStream2.map(new StringToWaterSensor());

        MapStateDescriptor<String, Double> mapStateDescriptor = new MapStateDescriptor<>("vc", String.class, Double.class);

        // TODO 4.将阈值流转换为广播流
        BroadcastStream<WaterSensor> broadcastDS = vcDS.broadcast(mapStateDescriptor);

        // TODO 5.将数据流与广播流进行连接
        BroadcastConnectedStream<WaterSensor, WaterSensor> connectDS = waterSensorDS.connect(broadcastDS);

        // TODO 6.对连接流进行处理  分流
        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("water-sensor") {
        };
        SingleOutputStreamOperator<WaterSensor> processDS = connectDS.process(new BroadcastProcessFunction<WaterSensor, WaterSensor, WaterSensor>() {

            @Override
            public void processElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, WaterSensor, WaterSensor>.ReadOnlyContext ctx, Collector<WaterSensor> out) throws Exception {

                ReadOnlyBroadcastState<String, Double> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                Double vc = broadcastState.get("vc");

                if (vc == null) {
                    vc = 30.0D;
                }

                if (value.getVc() > vc) {
                    ctx.output(outputTag, value);
                } else {
                    out.collect(value);
                }
            }

            @Override
            public void processBroadcastElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {

                BroadcastState<String, Double> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                broadcastState.put("vc", value.getVc());
            }
        });

        // TODO 7.获取侧输出流数据&打印
        processDS.print("Main>>>>>");
        processDS.getSideOutput(outputTag).print("Side>>>>>");

        // TODO 8.启动任务
        env.execute();
    }
}
