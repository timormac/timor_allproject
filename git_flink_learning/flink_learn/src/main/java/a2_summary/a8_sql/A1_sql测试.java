package a2_summary.a8_sql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author Timor
 * @Date 2024/2/28 10:17
 * @Version 1.0
 */
public class A1_sql测试 {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

         /**
         * 字段和类型之间必须只有一个空格
         */
         String kf_conn = "CREATE TABLE kafka_maxwell( \n" +
                 "`table` string,\n" +
                "  ts bigint , \n" +
                " data map<string, string>  \n " +
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


        //sink_sql报错找不到mock_order，逻辑没问题啊
        //TODO 只创建连接器,不执行sqlQuery的话,和createTemporyView是没有真正的表的，sink用到的必须是真正跑的表


        tableEnv.executeSql(kf_conn);
        tableEnv.executeSql(mysql_conn);

        Table mock_order = tableEnv.sqlQuery(" select * from kafka_maxwell where  `table` = 'mock_order' ");
        tableEnv.createTemporaryView("mock_order",mock_order);


        //TODO　一直报错未解决
        String excute_sql = "select * \n" +
                "from mock_order  \n" +
                "lookup join mock_cupon \n" +
                "on mock_order.data['orderno'] = mock_cupon.orderno ";

        System.out.println(excute_sql);

        TableResult tableResult = tableEnv.executeSql(excute_sql);
        tableResult.print();


//        String sink_print="CREATE TABLE sink (\n" +
//                "    orderno string,\n" +
//                "    activity string \n" +
//                ") WITH (\n" +
//                "'connector' = 'print' \n" +
//                ");\n";
//
//        String sink_sql= "insert into sink \n " +
//                "select  mock_order.data['orderno'], \n" +
//                "mock_cupon.activity \n" +
//                "from mock_order  \n " +
//                "lookup join mock_cupon  \n " +
//                "on mock_order.data['orderno'] = mock_cupon.orderno  \n ";

    }

}
