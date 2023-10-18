package com.atguigu.app.day04;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Flink03_Window_KeyedWindow {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        // TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        // TODO 4.按照ID进行分组
        KeyedStream<WaterSensor, String> keyedDS = waterSensorDS.keyBy(WaterSensor::getId);

        // TODO 5.开窗
        // 10秒的滚动窗口
        // WindowedStream<WaterSensor, String, TimeWindow> windowDS = keyedDS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 6秒的滑动窗口,2秒的滑动步长
        // WindowedStream<WaterSensor, String, TimeWindow> windowDS = keyedDS.window(SlidingProcessingTimeWindows.of(Time.seconds(6), Time.seconds(2)));
        // 间隔5秒的会话窗口
        // WindowedStream<WaterSensor, String, TimeWindow> windowDS = keyedDS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));
        // 5条数据的滚动窗口
        // WindowedStream<WaterSensor, String, GlobalWindow> windowDS = keyedDS.countWindow(5);
        // 6条数据的滑动窗口,2条数据的滑动步长
        WindowedStream<WaterSensor, String, GlobalWindow> windowDS = keyedDS.countWindow(6, 2);

        //TODO 6.窗口内部（每个key有独立的窗口）计算每个传感器水位线之和
        SingleOutputStreamOperator<WaterSensor> sumDS = windowDS.sum("vc");

        // TODO 7.打印结果
        sumDS.print();

        // TODO 8.启动任务
        env.execute();
    }
}
