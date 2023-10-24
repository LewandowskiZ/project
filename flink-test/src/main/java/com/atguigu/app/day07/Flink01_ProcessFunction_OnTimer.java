package com.atguigu.app.day07;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import com.sun.xml.internal.bind.v2.TODO;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink01_ProcessFunction_OnTimer {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        // TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        // TODO 4.按照ID进行分组  在接收到数据5秒以后输出一条数据,且将该数据输出到侧输出流
        OutputTag<String> outputTag = new OutputTag<String>("side") {
        };
        SingleOutputStreamOperator<WaterSensor> processDS = waterSensorDS.keyBy(WaterSensor::getId).process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {

                TimerService timerService = ctx.timerService();

                timerService.registerProcessingTimeTimer(timerService.currentProcessingTime() + 5000L);

                out.collect(value);
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, WaterSensor>.OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {

                ctx.output(outputTag, "5秒到了！");

                TimerService timerService = ctx.timerService();

                timerService.deleteProcessingTimeTimer(timestamp);
            }
        });

        // TODO 5.打印数据
        processDS.print("Main>>>>>");
        processDS.getSideOutput(outputTag).print("Side>>>>>");

        // TODO 6.启动任务
        env.execute();
    }
}
