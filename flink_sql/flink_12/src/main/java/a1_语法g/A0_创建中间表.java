package a1_语法g;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Title: A0_创建中间表
 * @Package: grammer
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/15 00:41
 * @Version:1.0
 */
public class A0_创建中间表 {
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



        /**
         * 创建表要么用with，想直接create table t1 select * from t2不可以
         * 但是创建视图,create  TEMPORARY VIEW tb1 select * from t2是可以的
         * 创建视图2种方式:sql和调用方法
         */


        //TODO   方式1： 先query查表，然后调createTemporaryView方法
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

        //TODO   方式2： 直接sql
        //TODO  注意 因为`data` map<string, string> ,所以status是String形式,所以要status = '0' 如果是status = 0没有数据
        String sql1 = "create temporary view tmp1 as " +
                "select " +
                "orderno , " +
                "spuid , " +
                "status " +
                "from mock_order " +
                "where status='1' ";

        System.out.println(sql1);
        //执行sql
        tableEnv.executeSql(sql1);


        String sql2 = " select * from tmp1 " ;
        System.out.println( sql2);

        TableResult tableResult = tableEnv.executeSql(sql2);
        tableResult.print();


    }
}
