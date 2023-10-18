package com.atguigu.app.day04;

import com.atguigu.bean.VcAcc;
import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Flink14_Window_WindowFunction_Aggregate {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        // TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        // TODO 4.分组开窗
        // WindowedStream<WaterSensor, String, TimeWindow> windowDS = waterSensorDS.keyBy(WaterSensor::getId).window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
        WindowedStream<WaterSensor, String, TimeWindow> windowDS = waterSensorDS.keyBy(WaterSensor::getId).window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));

        // TODO 5.使用Aggregate实现增量聚合
        SingleOutputStreamOperator<WaterSensor> aggregateDS = windowDS.aggregate(new AggregateFunction<WaterSensor, VcAcc, WaterSensor>() {

            // 中间状态的初始化
            @Override
            public VcAcc createAccumulator() {
                return new VcAcc("", 0.0D, 0);
            }

            // 逐行累加
            @Override
            public VcAcc add(WaterSensor waterSensor, VcAcc vcAcc) {
                return new VcAcc(waterSensor.getId(), waterSensor.getVc() + vcAcc.getSumVc(), vcAcc.getCountVc() + 1);
            }

            // 获取最终结果的方法
            @Override
            public WaterSensor getResult(VcAcc vcAcc) {
                return new WaterSensor(vcAcc.getId(), System.currentTimeMillis(), vcAcc.getSumVc() / vcAcc.getCountVc());
            }

            @Override
            public VcAcc merge(VcAcc vcAcc, VcAcc acc1) {
                System.out.println("VcAcc>>>>");
                return new VcAcc(vcAcc.getId(), vcAcc.getSumVc() + acc1.getSumVc(), vcAcc.getCountVc() + acc1.getCountVc());
            }
        });

        // TODO 6.打印结果
        aggregateDS.print();

        // TODO 7.启动任务
        env.execute();
    }
}
