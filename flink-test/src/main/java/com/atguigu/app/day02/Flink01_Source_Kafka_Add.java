package com.atguigu.app.day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class Flink01_Source_Kafka_Add {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 8081);
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        executionEnvironment.setParallelism(1);

        // 从Kafka读取数据
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test01");
        DataStreamSource<String> dataStreamSource = executionEnvironment.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties));

        dataStreamSource.print();

        executionEnvironment.execute();
    }
}
