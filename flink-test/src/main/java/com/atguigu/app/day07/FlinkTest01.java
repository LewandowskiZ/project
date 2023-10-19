package com.atguigu.app.day07;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FlinkTest01 {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        // TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        OutputTag<String> outputTag = new OutputTag<String>("warn") {
        };

        SingleOutputStreamOperator<WaterSensor> processDS = waterSensorDS.keyBy(WaterSensor::getId).process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

            private ListState<Double> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(new ListStateDescriptor<Double>("continue-three-vc-value", Double.class));
            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {

                if (value.getVc() > 10) {
                    listState.add(value.getVc());
                } else {
                    listState.clear();
                }

                Iterator<Double> iterator = listState.get().iterator();
                List<Double> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    list.add(iterator.next());
                }

                if (list.size() >= 3) {
                    ctx.output(outputTag, "连续3条数据的VC都超过10");
                }

                out.collect(value);
            }
        });

        processDS.print("Main>>>>>");
        processDS.getSideOutput(outputTag).print("Side>>>>>");

        env.execute();
    }
}
