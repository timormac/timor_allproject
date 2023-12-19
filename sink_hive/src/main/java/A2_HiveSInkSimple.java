import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Author Timor
 * @Date 2023/12/14 21:43
 * @Version 1.0
 */
public class A2_HiveSInkSimple {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tbEnv = StreamTableEnvironment.create(env);


        //直接就创建了一个kafka的表
        tbEnv.executeSql("" +
                "CREATE TABLE kafkaTb( \n" +
                "  `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
                "  `partition` BIGINT METADATA VIRTUAL,\n" +
                "  `offset` BIGINT METADATA VIRTUAL,\n" +
                "id int, \n" +
                "ts bigint , \n" +
                "vc int )\n" +
                "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'properties.bootstrap.servers' = 'project1:9092',\n" +
                "  'properties.group.id' = 'group_test1',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'sink.partitioner' = 'fixed',\n" +
                "  'topic' = 'offset_test1',\n" +
                "  'format' = 'json'\n" +
                ")");

        // 创建 HiveCatalog
        String name = "myhive";  // Catalog 名称
        String defaultDatabase = "personal_db_demo";  // 默认数据库
        String hiveConfDir = "/C:\\Users\\lpc\\Desktop\\hive-conf";  // Hive 配置文件目录

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tbEnv.registerCatalog(name, hive);
        tbEnv.useCatalog(name);

        //一直报注册的表找不到

        tbEnv.executeSql(" insert into  myhive.t1  select id from kafkaTb ");



        //没用到算子不要执行这个，不然报错
        env.execute();


    }
}
