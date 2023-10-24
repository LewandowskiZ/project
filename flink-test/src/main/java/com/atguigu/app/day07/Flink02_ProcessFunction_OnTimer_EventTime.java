package com.atguigu.app.day07;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink02_ProcessFunction_OnTimer_EventTime {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        // TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        OutputTag<String> outputTag = new OutputTag<String>("side") {
        };
        SingleOutputStreamOperator<WaterSensor> processDS = waterSensorDS
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {

                        return element.getTs() * 1000L;
                    }
                })).keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {

                        TimerService timerService = ctx.timerService();

                        timerService.registerEventTimeTimer(timerService.currentWatermark() * 1000L + 5000L);

                        out.collect(value);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, WaterSensor>.OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {

                        ctx.output(outputTag, "5秒到了！");

                        TimerService timerService = ctx.timerService();

                        timerService.deleteEventTimeTimer(timestamp);
                    }
                });

        // TODO 5.打印数据
        processDS.print("Main>>>>>");
        processDS.getSideOutput(outputTag).print("Side>>>>>");

        // TODO 6.启动任务
        env.execute();
    }
}
