package com.atguigu.app.day03;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink09_Transform_Union {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取端口数据
        DataStreamSource<String> socketTextStream1 = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> socketTextStream2 = env.socketTextStream("hadoop102", 8888);

        // TODO 3.合并两个端口读取到的数据 将每行数据转换为JavaBean对象 打印数据
        socketTextStream1.union(socketTextStream2).map(new StringToWaterSensor()).print();

        // TODO 4.启动任务
        env.execute();
    }
}
