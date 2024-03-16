package demos;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Title: A3_test3
 * @Package: demos
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/14 14:20
 * @Version:1.0
 */
public class A3_test3 {

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
        String excute_sql ="select \n" +
                "spuid,status,rk\n" +
                "from(\n" +
                "select \n" +
                "spuid,status,\n" +
                "row_number() over( partition by spuid,status order by status) as rk\n" +
                "from mock_order\n" +
                "group by spuid,status\n" +
                ") tmp\n" +
                "where rk < 10\n";
        System.out.println(excute_sql);

        TableResult tableResult = tableEnv.executeSql(excute_sql);
        tableResult.print();



    }
}
