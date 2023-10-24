package com.atguigu.app.day10;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

public class Flink05_WaterSensorId_Count_Window_4 {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.从端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //TODO 4.按照ID进行分区去重
        SingleOutputStreamOperator<Tuple2<String, Integer>> distinctDS = waterSensorDS.keyBy(WaterSensor::getId)
                .flatMap(new RichFlatMapFunction<WaterSensor, Tuple2<String, Integer>>() {

                    private ValueState<String> valueState;
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

                        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("value-state", String.class);
                        StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.minutes(1))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build();
                        stateDescriptor.enableTimeToLive(ttlConfig);
                        valueState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void flatMap(WaterSensor value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        //取出状态数据
                        String state = valueState.value();

                        long ts = System.currentTimeMillis();
                        //获取分钟
                        String dateTime = sdf.format(ts);

                        if (state == null || !state.equals(dateTime)) {
                            //更新状态并输出数据
                            valueState.update(dateTime);
                            out.collect(new Tuple2<>("id", 1));
                        }
                    }
                });

        //TODO 5.开窗聚合
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> resultDS = distinctDS.windowAll(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(1)))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        value1.f1 = value1.f1 + value2.f1;
                        return value1;
                    }
                }, new AllWindowFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple2<String, Integer>> values, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                        //获取窗口信息
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                        out.collect(new Tuple3<>(sdf.format(window.getStart()),
                                sdf.format(window.getEnd()),
                                values.iterator().next().f1));
                    }
                });

        //TODO 6.打印输出
        resultDS.print();

        //TODO 7.启动
        env.execute();

    }

}
