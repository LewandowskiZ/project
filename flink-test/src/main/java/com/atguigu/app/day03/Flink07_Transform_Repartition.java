package com.atguigu.app.day03;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink07_Transform_Repartition {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // TODO 2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        // TODO 3.使用Map算子将其并行化
        SingleOutputStreamOperator<String> mapDS = socketTextStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        });

        mapDS.print("mapDS>>>>>");

        // TODO 4.重分区并修改并行度,打印
        // 随机重分区
        // mapDS.shuffle().print("Shuffle>>>>>").setParallelism(4);
        // 单个 并行度 轮询/负载均衡
        // mapDS.rebalance().print("rebalance>>>>").setParallelism(4);
        // 单个 并行度 局部轮询 即 只能轮询个别分区
        // mapDS.rescale().print("rescale>>>>").setParallelism(4);
        // 广播
        // mapDS.broadcast().print("broadcast>>>").setParallelism(4);
        // 全局 即 将所有数据发往一个分区 通常为第一个分区
        // mapDS.global().print("global>>>>").setParallelism(4);

        // 要求上下游并行度必须一致
        mapDS.forward().print("forward>>>>");

        // TODO 5.启动任务
        env.execute();
    }
}
