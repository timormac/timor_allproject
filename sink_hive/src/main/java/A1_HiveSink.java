import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Author Timor
 * @Date 2023/12/14 19:56
 * @Version 1.0
 */
public class A1_HiveSink {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tbEnv = StreamTableEnvironment.create(env);

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

        DataStreamSource<String> kafkaDs = env.addSource(kafkaSource, TypeInformation.of(String.class));
        //变成POJO类,不然没法select字段

        SingleOutputStreamOperator<FlinkSinkDao> map = kafkaDs.map(x -> new FlinkSinkDao(x));


        //把kafka流注册成表
        Table kafkaTb = tbEnv.fromDataStream(map);
        //创建临时视图
        tbEnv.createTemporaryView("kafkaView",kafkaTb);




        // 创建 HiveCatalog
        String name = "myhive";  // Catalog 名称
        String defaultDatabase = "personal_db_demo";  // 默认数据库
        String hiveConfDir = "/C:\\Users\\lpc\\Desktop\\hive-conf";  // Hive 配置文件目录

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tbEnv.registerCatalog("myhive", hive);


        // 设置当前 catalog 和数据库
        tbEnv.useCatalog("myhive");
        tbEnv.useDatabase("personal_db_demo");

        tbEnv.executeSql(" insert into  myhive.t1  select id from kafkaView ");

        //没用到算子不要执行这个，不然报错
        env.execute();


    }





}
