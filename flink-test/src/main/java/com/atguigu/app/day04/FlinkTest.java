package com.atguigu.app.day04;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class FlinkTest {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> sourceDS = env.fromSource(KafkaSource.<String>builder()
                        .setBootstrapServers("hadoop102:9092")
                        .setTopics("test")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setGroupId("FlinkTest")
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .build(),
                WatermarkStrategy.noWatermarks(),
                "kafka-source");

        WindowedStream<WaterSensor, String, TimeWindow> windowDS = sourceDS.map(new StringToWaterSensor()).keyBy(WaterSensor::getId).window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<WaterSensor> maxDS = windowDS.maxBy("vc");

        maxDS.addSink(
                JdbcSink.sink(
                        "INSERT INTO ws VALUES(?,?,?)",
                        new JdbcStatementBuilder<WaterSensor>() {
                            @Override
                            public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                                preparedStatement.setString(1, waterSensor.getId());
                                preparedStatement.setLong(2, waterSensor.getTs());
                                preparedStatement.setDouble(3, waterSensor.getVc());
                            }
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1)
                                .withBatchIntervalMs(1000)
                                .withMaxRetries(2)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://localhost:3306/atguigu?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                                .withUsername("root")
                                .withPassword("123456")
                                .build()
                )
        );

        env.execute();
    }
}
