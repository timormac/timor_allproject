package grammer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Title: A3_滚动窗口
 * @Package: grammer
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/14 23:26
 * @Version:1.0
 */
public class A3_滚动窗口 {
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

        String  mock_order_sql = "create temporary view mock_order as  " +
                "select " +
                "data['orderno'] as orderno , " +
                "data['spuid'] as spuid ,"+
                "data['create_time'] as create_time , "+
                "data['price'] as price  " +
                " from kafka_maxwell" +
                " where `table` = 'mock_order' ";

         tableEnv.executeSql(mock_order_sql);



        /**
         * 只开窗,group by
         * TUMBLE_START/TUMBLE_END窗口开始和结束时间
         * TUMBLE_PROCTIME窗口关闭后，触发计算的时间
         */

//         String excute_sql = "select \n" +
//                 "spuid , \n" +
//                 "count(*) as num ,\n" +
//                 "TUMBLE_START( proctime(), INTERVAL '20' second) as w_start,\n" +
//                 "TUMBLE_END( proctime(), INTERVAL '20' second) as w_end,\n" +
//                 "TUMBLE_PROCTIME( proctime(), INTERVAL '20' second) as w_processtime \n" +
//                 "from mock_order \n" +
//                 "group by TUMBLE( proctime(), INTERVAL '20' second) ,spuid \n"
//                 ;


        //TODO 未分组只开窗
        String excute_sql = "select \n" +
                "count(*) as num ,\n" +
                "TUMBLE_START( proctime(), INTERVAL '20' second) as w_start,\n" +
                "TUMBLE_END( proctime(), INTERVAL '20' second) as w_end,\n" +
                "TUMBLE_PROCTIME( proctime(), INTERVAL '20' second) as w_processtime \n" +
                "from mock_order \n" +
                "group by TUMBLE( proctime(), INTERVAL '20' second)\n"
                ;

        System.out.println( excute_sql);

        TableResult tableResult = tableEnv.executeSql(excute_sql);
        tableResult.print();


    }
}
