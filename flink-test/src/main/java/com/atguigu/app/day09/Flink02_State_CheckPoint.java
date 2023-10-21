package com.atguigu.app.day09;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class Flink02_State_CheckPoint {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 开启CheckPoint
        env.enableCheckpointing(5000L);  //生产环境中设置为分钟  10Min

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //1.2 设置存储目录  目录会自动创建
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/flink/checkPoint");
        //1.3 设置精准一次
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //1.4 设置超时时间  评估数据量,写出时间
        checkpointConfig.setCheckpointTimeout(10000L);
        //1.5 设置最大并发检查点个数
        //checkpointConfig.setMaxConcurrentCheckpoints(2);
        //1.6 设置检查点之间最小间隔  那么次数任务中将不会再出现并发检查点
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
        //1.7 开启Cancel外部持久化存储
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); //保留HDFS上的 CheckPoint数据
        //checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION); //删除HDFS上的 CheckPoint数据
        //1.8 设置检查点连续失败次数  默认0次,即检查点失败任务挂掉
        checkpointConfig.setTolerableCheckpointFailureNumber(3);
        //1.9 开启非对齐模式
        checkpointConfig.enableUnalignedCheckpoints();
        //1.10 设置对齐超时时间
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(5));

        //设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));

        // TODO 2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        // TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        waterSensorDS.keyBy(WaterSensor::getId).sum("vc").print();

        env.execute();
    }
}
