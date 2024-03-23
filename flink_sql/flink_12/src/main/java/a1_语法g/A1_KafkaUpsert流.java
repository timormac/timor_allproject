package a1_语法g;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Title: A1_KafkaUpsert流
 * @Package: grammer
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/14 23:16
 * @Version:1.0
 */
public class A1_KafkaUpsert流 {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String kf_conn = "CREATE TABLE kafka_maxwell( \n" +
                "`database` string, " +
                "`table` string, " +
                "`data` map<string, string>, " +
                "`type` string, " +
                "`ts` string  " +
                ")WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'topic' = 'maxwell',\n" +
                "  'properties.group.id' = 'atguigu',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ") \n";

        tableEnv.executeSql(kf_conn);


        String kf_sink = "CREATE TABLE kafka_sink( \n" +
                "`spuid` String , " +
                "`num` bigint ," +
                "PRIMARY KEY (spuid) NOT ENFORCED" +
                ")WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'topic' = 'kafka_sink',\n" +
                "  'properties.group.id' = 'atguigu',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ") \n";

        tableEnv.executeSql(kf_sink);


        Table mock_order = tableEnv.sqlQuery("select " +
                "data['orderno'] as orderno , " +
                "data['spuid'] as spuid ,"+
                "data['create_time'] as create_time , "+
                // "user_action_time AS PROCTIME() ,"+
                "data['status'] as status , " +
                "data['price'] as price  " +
                " from kafka_maxwell" +
                " where `table` = 'mock_order' ");
        tableEnv.createTemporaryView("mock_order",mock_order);


        //这个sq，每次断开就会输出一条
        String excute_sql = " insert into  kafka_sink " +
                "select \n" +
                "spuid,\n" +
                "count(*) as num\n" +
                "from mock_order\n" +
                "group by spuid" ;

        TableResult tableResult = tableEnv.executeSql(excute_sql);
        tableResult.print();

        /**
         * 流打印是+I -U +U格式，更新一条数据有2条
         *虽然有回退流,但是kakfa查看数据只有更新数据
         * {"spuid":"0","num":1}
         * {"spuid":"0","num":2}
         * {"spuid":"0","num":3}
         *
         * 并且实时消费为多条更新数据,后序from-beginning还是多条，没有合并成一条，不知道这个upsert的意义
         */




    }
}
