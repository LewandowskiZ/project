package com.atguigu.app.day03;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.xa.JdbcXaSinkFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Flink14_Sink_Mysql {

    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // TODO 2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        // TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        // TODO 4.将数据写出到MySQL
        waterSensorDS
                .keyBy(WaterSensor::getId)
                .maxBy("vc")
                .addSink(JdbcSink.sink("INSERT INTO ws VALUES(?,?,?) ON DUPLICATE KEY UPDATE `ts` = ? , `vc` = ?",
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
                                .withBatchSize(1)
                                .withBatchIntervalMs(1000)
                                .withMaxRetries(2)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://localhost:3306/atguigu?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                                .withUsername("root")
                                .withPassword("123456")
                                .build()));

        // TODO 5.启动任务
        env.execute();
    }
}
