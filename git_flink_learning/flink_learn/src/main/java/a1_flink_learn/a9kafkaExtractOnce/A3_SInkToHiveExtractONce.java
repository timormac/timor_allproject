package a1_flink_learn.a9kafkaExtractOnce;

import a1_flink_learn.dao.AcidTableDao;
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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Author Timor
 * @Date 2023/12/14 17:18
 * @Version 1.0
 */
public class A3_SInkToHiveExtractONce {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 创建HiveCatalog
        String catalogName = "myHive";
        String defaultDatabase = "personal_db_demo";
        String hiveConfDir = "/opt/module/hive/conf";
        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog(catalogName, hiveCatalog);
        tableEnv.useCatalog(catalogName);



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
        SingleOutputStreamOperator<AcidTableDao> map = kafkaDs.map(x -> new AcidTableDao(x, "asd"));


        //配置hive信息
        System.setProperty("hive.metastore.uris", "thrift://project1:9083");
        System.setProperty("hive.exec.dynamic.partition.mode", "nonstrict");
        System.setProperty("hive.exec.dynamic.partition", "true");
        System.setProperty("hive.exec.max.dynamic.partitions", "100000");
        System.setProperty("hive.exec.max.dynamic.partitions.pernode", "100000");
        System.setProperty("hive.metastore.execute.setugi", "true");
        System.setProperty("fs.defaultFS", "hdfs://project1:8020");


        //把kafka流注册成表
        Table kafkaTb = tableEnv.fromDataStream(map);
        //创建临时视图
        tableEnv.createTemporaryView("kafkaView",kafkaTb);
        //从kafka视图中选数据


        tableEnv.executeSql("INSERT INTO myHive.acid_table  partition(dt = '2023-12-14' )"   + " ( SELECT id, name FROM  kafkaView)" );


        // 执行作业
        env.execute("Flink Hive Demo");






    }
}
