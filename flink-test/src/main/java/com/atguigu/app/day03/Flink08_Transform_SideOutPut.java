package com.atguigu.app.day03;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink08_Transform_SideOutPut {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // TODO 2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        // TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());


        // TODO 4.使用ProcessAPI进行分流,大于30的一个流 主流,小于等于30的一个流  侧流
        // 通过匿名实现类防止泛型丢失，因为继承后数据类型将会被确定
        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("WaterSensor") {
        };
        SingleOutputStreamOperator<WaterSensor> processDS = waterSensorDS.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor waterSensor, ProcessFunction<WaterSensor, WaterSensor>.Context context, Collector<WaterSensor> collector) throws Exception {
                if (waterSensor.getVc() > 30) {
                    // 主流
                    collector.collect(waterSensor);
                } else {
                    // 侧流
                    context.output(outputTag, waterSensor);
                }
            }
        });

        // TODO 5.打印数据流
        processDS.print("processDS>>>");
        processDS.getSideOutput(outputTag).print();

        // TODO 6.启动任务
        env.execute();
    }
}
