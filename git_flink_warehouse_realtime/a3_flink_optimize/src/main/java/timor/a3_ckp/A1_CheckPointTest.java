package timor.a3_ckp;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Author Timor
 * @Date 2023/12/19 21:18
 * @Version 1.0
 */
public class A1_CheckPointTest {

    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置状态后端为RocksDB
        //env.setStateBackend( new rocksdb );


        //这样登录hdfs时为lpc的用户名
        System.setProperty("HADOOP_USER_NAME","lpc");
        //开启checkpoint，模式还有个至少一次的选项
        env.enableCheckpointing(40*1000L, CheckpointingMode.EXACTLY_ONCE);
        //获取状态后端配置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //设置chekpint存储路径
        checkpointConfig.setCheckpointStorage("hdfs://project1:8020/flink_need/checkpoint_dir");
        //配置文件
        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "project1:9092,project2:9092,project3:9092");
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my_group2");
        //把自动提交关闭，交给flink,开启checkpoint后统一由checkpoint统一提交
        p.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 禁用自动重置偏移量
        //如果用FlinkKafkaConsumer创建的消费者，就能设定这个参数，不过用kafkaSouce创建不不同
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("offset_test1" , new SimpleStringSchema(),p);
        //这里设置flase方便验证,开始设置为false,kafka那里找不到消费者组my_group1
        //执行查看消费者组的指令，看到总共22数据，当前offset为12，已经确定offset统一由checkpoint提交了
        //当任务再开启时,会从上次的offset执行,相当于重复执行了。因为上次已经打印过一次，不过没有提交，开启后又打印一次了
        kafkaSource.setCommitOffsetsOnCheckpoints(true);
        DataStreamSource<String> kafkaDs;
        kafkaDs = env.addSource(kafkaSource, TypeInformation.of(String.class));




    }

}

