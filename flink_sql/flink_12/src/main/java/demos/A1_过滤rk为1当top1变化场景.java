package demos;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Title: A1_Demo
 * @Package: demos
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/12 23:08
 * @Version:1.0
 */
public class A1_过滤rk为1当top1变化场景 {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String kf_conn = "CREATE TABLE kafka_maxwell( \n" +
                "`database` string, " +
                "`table` string, " +
                "`data` map<string, string>, " +
                "`type` string, " +
                "`ts` string , " +
                "inner_time AS PROCTIME() "+  //加一个进入时间作为时间
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
                "data['price'] as price  " +
                " from kafka_maxwell" +
                " where `table` = 'mock_order' ");
        tableEnv.createTemporaryView("mock_order",mock_order);

        String excute_sql =" " +
                "select * " +
                "from ( " +
                    "select " +
                    "orderno, " +
                    "spuid, " +
                    "price , " +
                    "row_number() over( partition by spuid order by price) as rk  " +
                    "from mock_order " +
                ") " +
                "where rk = 1 " ;
        System.out.println(excute_sql);

        //TODO 注意当rk=1的数据发生变化时，后出现2个数据一个是撤回流，一个是新的rk=1
//| +I |                   D334282968|6 |                              2 |                          844.9 |                    1 |
//| +I |                   D334287215|9 |                              0 |                         339.26 |                    1 |
//| +I |                   D334289341|8 |                              1 |                          53.15 |                    1 |
//| -D |                   D334287215|9 |                              0 |                         339.26 |                    1 |
//| +I |                   D334297841|6 |                              0 |                         181.22 |                    1 |
//

        TableResult tableResult = tableEnv.executeSql(excute_sql);

        tableResult.print();

    }
}
