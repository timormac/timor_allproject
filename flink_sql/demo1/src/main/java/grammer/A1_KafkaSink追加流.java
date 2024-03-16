package grammer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Title: A1_KafkaSink追加流
 * @Package: grammer
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/14 23:15
 * @Version:1.0
 */
public class A1_KafkaSink追加流 {
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


        //TODO 和upsert流with写法不太一样
        String kf_sink = "CREATE TABLE kafka_sink( \n" +
                "`spuid` String , " +
                "`orderno` string " +
                ")WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'topic' = 'kafka_sink',\n" +
                "  'format' = 'json' \n" +
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



        String excute_sql = "insert into kafka_sink  \n" +
                "select \n" +
                "spuid,\n" +
                "orderno\n" +
                "from mock_order";

        System.out.println( excute_sql);

        TableResult tableResult = tableEnv.executeSql(excute_sql);
        tableResult.print();



    }
}
