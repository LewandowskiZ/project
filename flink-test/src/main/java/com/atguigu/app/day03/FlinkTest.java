package com.atguigu.app.day03;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class FlinkTest {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setTopics("test")
                .setGroupId("testGroup")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaSourceDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "test");


        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("waterSensor") {
        };
        SingleOutputStreamOperator<WaterSensor> processDS = kafkaSourceDs
                .map(new StringToWaterSensor())
                .process(new ProcessFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor waterSensor, ProcessFunction<WaterSensor, WaterSensor>.Context context, Collector<WaterSensor> collector) throws Exception {
                        if (waterSensor.getVc() > 30) {
                            collector.collect(waterSensor);
                        } else {
                            context.output(outputTag, waterSensor);
                        }
                    }
                });

        processDS
                .map(new MapFunction<WaterSensor, String>() {
                    @Override
                    public String map(WaterSensor waterSensor) throws Exception {
                        return waterSensor.toString();
                    }
                })
                .sinkTo(KafkaSink.<String>builder()
                        .setRecordSerializer(new KafkaRecordSerializationSchema<String>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(String s, KafkaSinkContext kafkaSinkContext, Long aLong) {
                                return new ProducerRecord<>("tests", s.getBytes());
                            }
                        })
                        .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                        .build());

        processDS.getSideOutput(outputTag)
                .keyBy(WaterSensor::getId)
                .minBy("vc")
                .addSink(JdbcSink.sink(
                        "INSERT INTO ws VALUES(?,?,?) ON DUPLICATE KEY UPDATE `ts` = ? , `vc` = ?",
                        new JdbcStatementBuilder<WaterSensor>() {
                            @Override
                            public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                                preparedStatement.setString(1, waterSensor.getId());
                                preparedStatement.setLong(2, waterSensor.getTs());
                                preparedStatement.setDouble(3, waterSensor.getVc());
                                preparedStatement.setLong(4, waterSensor.getTs());
                                preparedStatement.setDouble(5, waterSensor.getVc());
                            }
                        },
                        JdbcExecutionOptions.builder()
                                .withMaxRetries(2)
                                .withBatchIntervalMs(1000)
                                .withBatchSize(1)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://localhost:3306/atguigu?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                                .withUsername("root")
                                .withPassword("123456")
                                .build()
                ));


        env.execute();
    }
}
