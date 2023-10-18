package com.atguigu.app.day03;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class Flink13_Sink_Kafka_SinkTo {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // TODO 2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        // TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        // TODO 4.将数据写出到Kafka
//        KafkaSink<String> kafkaSink = KafkaSink
//                .<String>builder()
//                .setBootstrapServers("hadoop102:9092")
//                .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<String>()
//                        .setTopic("test")
//                        .setValueSerializationSchema(new SimpleStringSchema())
//                        .build())
//                .build();

        KafkaSink<String> kafkaSink = KafkaSink
                .<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setRecordSerializer(new KafkaRecordSerializationSchema<String>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String s, KafkaSinkContext kafkaSinkContext, Long aLong) {
                        return new ProducerRecord<>("test", s.getBytes());
                    }
                })
                .build();

        waterSensorDS.map(JSON::toJSONString).sinkTo(kafkaSink);

        // TODO 5.启动任务
        env.execute();
    }
}
