package com.atguigu.app.day07;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.crypto.spec.OAEPParameterSpec;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class FlinkTest02 {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        // TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        SingleOutputStreamOperator<String> mapDS = waterSensorDS.keyBy(WaterSensor::getId).map(new RichMapFunction<WaterSensor, String>() {

            private ListState<Double> listState;

            @Override
            public void open(Configuration parameters) throws Exception {

                listState = getRuntimeContext().getListState(new ListStateDescriptor<Double>("top3", Double.class));
            }

            @Override
            public String map(WaterSensor value) throws Exception {

                listState.add(value.getVc());

                List<Double> list = new ArrayList<>();
                Iterator<Double> iterator = listState.get().iterator();
                while (iterator.hasNext()) {

                    list.add(iterator.next());
                }

                list.sort(new Comparator<Double>() {
                    @Override
                    public int compare(Double o1, Double o2) {
                        return o2.compareTo(o1);
                    }
                });

                listState.clear();
                StringBuilder outPut = new StringBuilder().append(value.getId()).append(":");
                for (int i = 0; i < Math.min(list.size(), 3); i++) {

                    outPut.append(list.get(i)).append(",");
                    listState.add(list.get(i));
                }

                return outPut.deleteCharAt(outPut.length() - 1).toString();
            }
        });

        mapDS.print();

        env.execute();
    }
}
