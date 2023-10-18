package com.atguigu.app.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Flink01_WordCountBatch {

    public static void main(String[] args) throws Exception {

        // 获取Flink执行环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据   按照行读取数据
        DataSource<String> lineDS = executionEnvironment.readTextFile("input/word.txt");

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
                .groupBy(0)
                .sum(1)
                .print();
    }
}
