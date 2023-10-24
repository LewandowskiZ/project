package com.atguigu.app.day06;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Flink07_IntervalJoin {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.从端口读取数据
        //1001,12,23.5
        DataStreamSource<String> socketTextStream1 = env.socketTextStream("hadoop102", 8888);
        //1001,12,sensor_1
        SingleOutputStreamOperator<String> socketTextStream2 = env.socketTextStream("hadoop102", 9999)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        return Long.parseLong(element.split(",")[1]) * 1000L;
                    }
                }));

        //TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream1.map(new StringToWaterSensor()).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        }));

        //TODO 4.使用IntervalJoin
        SingleOutputStreamOperator<Tuple3<String, String, Double>> resultDS = waterSensorDS.keyBy(WaterSensor::getId)
                .intervalJoin(socketTextStream2.keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        return value.split(",")[0];
                    }
                }))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<WaterSensor, String, Tuple3<String, String, Double>>() {
                    @Override
                    public void processElement(WaterSensor left, String right, ProcessJoinFunction<WaterSensor, String, Tuple3<String, String, Double>>.Context ctx, Collector<Tuple3<String, String, Double>> out) throws Exception {
                        out.collect(new Tuple3<>(left.getId(), right.split(",")[2], left.getVc()));
                    }
                });

        //TODO 5.打印结果
        resultDS.print();

        //TODO 6.启动任务
        env.execute();

    }

}
