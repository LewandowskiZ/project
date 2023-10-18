package com.atguigu.app.day04;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink16_Window_WindowFunction_ReduceAndFull {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        // TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        WindowedStream<WaterSensor, String, TimeWindow> windowDS = waterSensorDS.keyBy(WaterSensor::getId).window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        // TODO 5.使用Reduce实现增量聚合
        SingleOutputStreamOperator<WaterSensor> reduceDS = windowDS.reduce(
                new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor waterSensor, WaterSensor t1) throws Exception {
                        return new WaterSensor(waterSensor.getId(), System.currentTimeMillis(), waterSensor.getVc() + t1.getVc());
                    }
                },
                new WindowFunction<WaterSensor, WaterSensor, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<WaterSensor> iterable, Collector<WaterSensor> collector) throws Exception {
                        WaterSensor waterSensor = iterable.iterator().next();
                        waterSensor.setId(waterSensor.getId() + " " + timeWindow.getStart());
                        collector.collect(waterSensor);
                    }
                }
        );

        // TODO 6.打印结果
        reduceDS.print();

        // TODO 7.启动任务
        env.execute();
    }
}
