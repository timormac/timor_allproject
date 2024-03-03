package a2_summary.a8_sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author Timor
 * @Date 2024/2/28 12:35
 * @Version 1.0
 */
public class A2_2个kafka流关联 {

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

        tableEnv.executeSql(kf_conn);


        Table mock_order = tableEnv.sqlQuery("select data from kafka_maxwell where `table` = 'mock_order' ");
        Table mock_cupon = tableEnv.sqlQuery("select data from kafka_maxwell where `table` = 'mock_cupon' ");

        tableEnv.createTemporaryView("mock_order",mock_order);
        tableEnv.createTemporaryView("mock_cupon",mock_cupon);

        String excute_sql =" select \n " +
                "mock_order.data['orderno'],\n" +
                "mock_order.data['spuid'],\n" +
                "mock_cupon.data['activity']\n" +
                "from mock_order \n" +
                "join mock_cupon \n" +
                "on mock_order.data['orderno'] = mock_cupon.data['orderno'] ";
        System.out.println(excute_sql);


        System.out.println(tableEnv.explainSql(excute_sql));


//        Table table = tableEnv.sqlQuery(excute_sql);
//        tableEnv.createTemporaryView("temp",table);
//
//        String sql = tableEnv.explainSql("select * from temp");
//
//        System.out.println(sql);

 //       TableResult tableResult = tableEnv.executeSql("select * from temp");

        TableResult tableResult = tableEnv.executeSql(excute_sql);


        tableResult.print();


    }



}
