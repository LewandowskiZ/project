package com.atguigu.app.day06;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class FlinkTest01 {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> socketTextStream2 = env.socketTextStream("hadoop102", 8888);

        WatermarkStrategy<String> stringWatermarkStrategy = WatermarkStrategy.<String>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        return Long.parseLong(element.split(",")[1]) * 1000L;
                    }
                });
        SingleOutputStreamOperator<String> watermarksDS = socketTextStream.assignTimestampsAndWatermarks(stringWatermarkStrategy);
        SingleOutputStreamOperator<String> watermarksDS2 = socketTextStream2.assignTimestampsAndWatermarks(stringWatermarkStrategy);

        DataStream<Tuple4<String, Long, Double, String>> applyDS = watermarksDS.join(watermarksDS2)
                .where(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        return value.split(",")[0];
                    }
                })
                .equalTo(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        return value.split(",")[0];
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<String, String, Tuple4<String, Long, Double, String>>() {
                    @Override
                    public Tuple4<String, Long, Double, String> join(String first, String second) throws Exception {
                        return new Tuple4<>(first.split(",")[0], Long.parseLong(first.split(",")[1]), Double.parseDouble(first.split(",")[2]), second.split(",")[2]);
                    }
                });

        applyDS.print();

        env.execute();
    }
}
