package com.atguigu.app.day02;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class Flink02_Source_Kafka_From {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 8081);
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        executionEnvironment.setParallelism(1);

        // 从Kafka读取数据
        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setTopics("test")
                .setGroupId("test01")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema()).build();
        DataStreamSource<String> dataStreamSource = executionEnvironment.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        dataStreamSource.print();

        executionEnvironment.execute();
    }
}
