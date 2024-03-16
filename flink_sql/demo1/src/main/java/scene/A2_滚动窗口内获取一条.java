package scene;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Title: A2_滚动窗口内获取一条
 * @Package: scene
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/15 07:05
 * @Version:1.0
 */
public class A2_滚动窗口内获取一条 {

    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String kf_conn = "CREATE TABLE kafka_maxwell( \n" +
                "`database` string, " +
                "`table` string, " +
                "`data` map<string, string>, " +
                "`type` string, " +
                " ptm AS PROCTIME() , " +
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
                "data['price'] as price , " +
                "ptm " +
                " from kafka_maxwell" +
                " where `table` = 'mock_order' ";

        tableEnv.executeSql(mock_order_sql);

        //TODO 目前sql无法执行
        String excute_sql = "SELECT\n" +
                "orderno,\n" +
                "spuid,\n" +
                "create_time,\n" +
                "ptm,\n" +
                "TUMBLE_START(ptm, INTERVAL '20' SECOND) as window_start,\n" +
                "TUMBLE_END(ptm, INTERVAL '20' SECOND) as window_end\n" +
                "FROM mock_order";

        System.out.println(excute_sql);

        TableResult tableResult = tableEnv.executeSql(excute_sql);
        tableResult.print();


    }
}
