package com.atguigu.app.day05;

import com.atguigu.bean.WaterSensor;
import com.atguigu.bean.WaterSensorWithWindowTime;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.Duration;

public class Flink08_Window_EventTime_Tumbling_ToMysql {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.从端口读取数据
        DataStreamSource<String> socketTextStream = env.fromSource(
                KafkaSource.<String>builder()
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .setTopics("test")
                        .setGroupId("flinkTest")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setBootstrapServers("hadoop102:9092")
                        .build(),
                WatermarkStrategy.noWatermarks(),
                "kafka-source"
        );

        //TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //TODO 4.提取时间戳生成WaterMark
        SingleOutputStreamOperator<WaterSensor> waterSensorWithWMDS = waterSensorDS.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        }));

        //TODO 5.分组开窗聚合
        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("late") {
        };
        SingleOutputStreamOperator<WaterSensorWithWindowTime> resultDS = waterSensorWithWMDS.keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(outputTag)
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        return new WaterSensor(value1.getId(), 0L, value1.getVc()+ value2.getVc());
                    }
                }, new WindowFunction<WaterSensor, WaterSensorWithWindowTime, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<WaterSensor> input, Collector<WaterSensorWithWindowTime> out) throws Exception {
                        WaterSensor waterSensor = input.iterator().next();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        out.collect(new WaterSensorWithWindowTime(waterSensor.getId(),
                                sdf.format(window.getStart()),
                                sdf.format(window.getEnd()),
                                waterSensor.getVc(),0));
                    }
                });

        resultDS.print("resultDS>>>>>");

        //TODO 6.打印结果
        SideOutputDataStream<WaterSensor> sideOutput = resultDS.getSideOutput(outputTag);

        //TODO 7.将侧输出流处理成与主流相同的数据流
        SingleOutputStreamOperator<WaterSensorWithWindowTime> resultWithSideDS = sideOutput.map(new MapFunction<WaterSensor, WaterSensorWithWindowTime>() {
            @Override
            public WaterSensorWithWindowTime map(WaterSensor value) throws Exception {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                long start = TimeWindow.getWindowStartWithOffset(value.getTs() * 1000L, 0L, 10 * 1000L);
                return new WaterSensorWithWindowTime(value.getId(),
                        sdf.format(start),
                        sdf.format(start + 10 * 1000L),
                        value.getVc(),1);
            }
        });

        //TODO 8.将两个流进行合并
        DataStream<WaterSensorWithWindowTime> mysqlDS = resultDS.union(resultWithSideDS);

        //TODO 9.将数据写出到MySQL
        mysqlDS.addSink(JdbcSink.<WaterSensorWithWindowTime>sink("INSERT INTO ws_result VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE `vc`= IF(0 = ?, ?, `vc` + ?)",
                new JdbcStatementBuilder<WaterSensorWithWindowTime>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensorWithWindowTime waterSensorWithWindowTime) throws SQLException {
                        preparedStatement.setString(3, waterSensorWithWindowTime.getId());
                        preparedStatement.setString(1, waterSensorWithWindowTime.getStt());
                        preparedStatement.setString(2, waterSensorWithWindowTime.getEdt());
                        preparedStatement.setDouble(4, waterSensorWithWindowTime.getVc());
                        preparedStatement.setDouble(5, waterSensorWithWindowTime.getFlag());
                        preparedStatement.setDouble(6, waterSensorWithWindowTime.getVc());
                        preparedStatement.setDouble(7, waterSensorWithWindowTime.getVc());
                    }
                }, new JdbcExecutionOptions.Builder()
                        .withMaxRetries(2)
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/atguigu?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()));

        //TODO 7.启动任务
        env.execute();
    }
}
