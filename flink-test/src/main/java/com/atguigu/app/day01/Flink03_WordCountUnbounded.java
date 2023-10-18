package com.atguigu.app.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink03_WordCountUnbounded {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment executionEnvironment1 = StreamExecutionEnvironment.getExecutionEnvironment();
        // 修改并行度
        executionEnvironment.setParallelism(1);
        executionEnvironment1.setParallelism(1);

        DataStreamSource<String> lineDS = executionEnvironment.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> lineDS1 = executionEnvironment1.socketTextStream("hadoop102", 7777);

        lineDS
                // 压平数据并转换为元组
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] split = s.split(" ");
                        for (String s1 : split) {
                            collector.collect(new Tuple2<>(s1, 1));
                        }
                    }
                })
                // 分组 聚合
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                })
                .sum(1)
                .print();

        lineDS1
                // 压平数据并转换为元组
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] split = s.split(" ");
                        for (String s1 : split) {
                            collector.collect(new Tuple2<>(s1, 1));
                        }
                    }
                })
                // 分组 聚合
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                })
                .sum(1)
                .print();

        // 启动任务 阻塞
        // executionEnvironment.execute();

        executionEnvironment.executeAsync();
        executionEnvironment1.execute();
    }
}
