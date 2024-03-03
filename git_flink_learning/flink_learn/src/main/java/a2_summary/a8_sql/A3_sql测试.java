package a2_summary.a8_sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author Timor
 * @Date 2024/2/28 13:21
 * @Version 1.0
 */
public class A3_sql测试 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String kf_conn = "CREATE TABLE kafka_maxwell( \n" +
                "`database` string, " +
                "`table` string, " +
                "`data` map<string, string>, " +
                "`type` string, " +
                "`ts` string "+
                ")WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'properties.bootstrap.servers' = 'project1:9092,project2:9092,project3:9092',\n" +
                "  'topic' = 'maxwell',\n" +
                "  'properties.group.id' = 'atguigu',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ") \n";

        String  mysql_conn = "CREATE TABLE mock_cupon\n" +
                "(\n" +
                "  id INT,\n" +
                "  cuponno string,\n" +
                "  orderno string,\n" +
                "  activity string\n" +
                ")WITH (\n" +
                "    'connector'='jdbc',\n" +
                "    'url' = 'jdbc:mysql://project1:3306/flink_warehouse_db?useUnicode=true&characterEncoding=utf8',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '121995',\n" +
                "    'connection.max-retry-timeout' = '60s',\n" +
                "    'table-name' = 'mock_cupon'\n" +
                ");\n";

        tableEnv.executeSql(mysql_conn);
        tableEnv.executeSql(kf_conn);

        Table mock_cupon = tableEnv.sqlQuery("select orderno,activity from mock_cupon");
        tableEnv.createTemporaryView(  "mysql_cupon",mock_cupon );



        Table mock_order = tableEnv.sqlQuery("select data from kafka_maxwell where `table` = 'mock_order' ");
        tableEnv.createTemporaryView("mock_order",mock_order);

        Table kf_mock_cupon = tableEnv.sqlQuery("select data from kafka_maxwell where `table` = 'mock_cupon' ");
        tableEnv.createTemporaryView("kf_mock_cupon",kf_mock_cupon);

//        String excute_sql =" select \n " +
//                "mock_order.data['orderno'],\n" +
//                "mock_order.data['spuid'],\n" +
//                "mysql_cupon.activity\n" +
//                "from mock_order \n" +
//                "lookup join mysql_cupon \n" +
//                "on mock_order.data['orderno'] = mysql_cupon.orderno ";

        //单独查没问题，lookup_join mysql中维度表，就找不到mock_order表
        String excute_sql = " select * from mock_order";

        System.out.println(excute_sql);

        TableResult tableResult = tableEnv.executeSql(excute_sql);

        tableResult.print();



    }
}
