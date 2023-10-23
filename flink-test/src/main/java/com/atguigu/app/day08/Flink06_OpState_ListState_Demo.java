package com.atguigu.app.day08;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.OpListStateMapFunction;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink06_OpState_ListState_Demo {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //开启CheckPoint
        env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flink-ck");

        //TODO 2.从端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //TODO 4.在map算子中计算数据的个数。
        SingleOutputStreamOperator<Integer> resultDS = waterSensorDS.map(new OpListStateMapFunction());

        //TODO 5.打印
        resultDS.print();

        //TODO 6.启动任务
        env.execute();
    }

}
