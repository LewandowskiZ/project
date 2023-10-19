package com.atguigu.app.day07;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class FlinkTest03 {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.从端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor()).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        }));


        //TODO 4.分组开窗增量聚合  10秒窗口大小,5秒滑动步长的窗口
        SingleOutputStreamOperator<Tuple3<String, Double, Integer>> reduceDS = waterSensorDS.map(new MapFunction<WaterSensor, Tuple2<Double, Integer>>() {
                    @Override
                    public Tuple2<Double, Integer> map(WaterSensor value) throws Exception {
                        return new Tuple2<>(value.getVc(), 1);
                    }
                }).keyBy(new KeySelector<Tuple2<Double, Integer>, Double>() {
                    @Override
                    public Double getKey(Tuple2<Double, Integer> value) throws Exception {
                        return value.f0;
                    }
                }).window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<Double, Integer>>() {
                    @Override
                    public Tuple2<Double, Integer> reduce(Tuple2<Double, Integer> value1, Tuple2<Double, Integer> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                }, new WindowFunction<Tuple2<Double, Integer>, Tuple3<String, Double, Integer>, Double, TimeWindow>() {
                    @Override
                    public void apply(Double aDouble, TimeWindow window, Iterable<Tuple2<Double, Integer>> input, Collector<Tuple3<String, Double, Integer>> out) throws Exception {
                        Tuple2<Double, Integer> next = input.iterator().next();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        out.collect(new Tuple3<>(sdf.format(window.getEnd()), next.f0, next.f1));
                    }
                });

        //TODO 5.按照窗口结束时间分组,将同一个窗口所有数据收集齐,排序取前两名输出
        SingleOutputStreamOperator<String> processDS = reduceDS.keyBy(new KeySelector<Tuple3<String, Double, Integer>, String>() {
            @Override
            public String getKey(Tuple3<String, Double, Integer> value) throws Exception {
                return value.f0;
            }
        }).process(new KeyedProcessFunction<String, Tuple3<String, Double, Integer>, String>() {

            private ListState<Tuple2<Double, Integer>> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(new ListStateDescriptor<>("top2", Types.TUPLE(Types.DOUBLE, Types.INT)));
            }

            @Override
            public void processElement(Tuple3<String, Double, Integer> value, KeyedProcessFunction<String, Tuple3<String, Double, Integer>, String>.Context ctx, Collector<String> out) throws Exception {

                List<Tuple2<Double, Integer>> list = new ArrayList<>();
                Iterator<Tuple2<Double, Integer>> iterator = listState.get().iterator();
                while (iterator.hasNext()) {
                    list.add(iterator.next());
                }

                if (list.size() == 0) {
                    TimerService timerService = ctx.timerService();
                    timerService.registerEventTimeTimer(timerService.currentWatermark() + 1000L);
                }

                listState.add(new Tuple2<>(value.f1, value.f2));
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple3<String, Double, Integer>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

                List<Tuple2<Double, Integer>> list = new ArrayList<>();
                Iterator<Tuple2<Double, Integer>> iterator = listState.get().iterator();
                while (iterator.hasNext()) {
                    list.add(iterator.next());
                }

                list.sort(new Comparator<Tuple2<Double, Integer>>() {
                    @Override
                    public int compare(Tuple2<Double, Integer> o1, Tuple2<Double, Integer> o2) {
                        //降序,后 - 前
                        return o2.f1 - o1.f1;
                    }
                });

                //输出前两名
                StringBuilder stringBuilder = new StringBuilder("水位线Top2为：");
                for (int i = 0; i < Math.min(2, list.size()); i++) {
                    Tuple2<Double, Integer> tuple2 = list.get(i);
                    stringBuilder
                            .append("\n")
                            .append(tuple2.f0)
                            .append("出现")
                            .append(tuple2.f1)
                            .append("次");
                }

                //清空集合
                list.clear();

                out.collect(stringBuilder.toString());
            }
        });

        //TODO 6.打印结果
        processDS.print();

        //TODO 7.启动任务
        env.execute();
    }
}
