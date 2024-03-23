package a3_场景demo;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Title: A1_每5分输出时间最近的价格最高的一条
 * @Package: scene
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/15 01:54
 * @Version:1.0
 */
public class A1_每5分输出最近最贵一条 {
    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String kf_conn = "CREATE TABLE kafka_maxwell( \n" +
                "`database` string, " +
                "`table` string, " +
                "`data` map<string, string>, " +
                "`type` string, " +
                "ptm AS PROCTIME() , " +
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

        /**
         * 需求:统计不同spuid下,最近30s内,卖的最贵的，时间最新的一条数据
         */

        //TODO 失败了,这个写法,没有groupby的字段无法获取
//        String excute_sql = "select \n" +
//                "orderno,\n" +
//                "spuid,\n" +
//                "price,\n" +
//                "create_time\n" +
//                "from(\n" +
//                "    select \n" +
//                "    orderno,\n" +
//                "    spuid,\n" +
//                "    price,\n" +
//                "    create_time,\n" +
//                "    row_number()over( partition by spuid  order by price desc, create_time desc  ) as rk\n" +
//                "    from  mock_order \n" +
//                "    group by TUMBLE( proctime(), INTERVAL '20' second)\n" +
//                ")\n" +
//                "where rk = 1";

        String excute_sql="SELECT *\n" +
                "FROM (\n" +
                "    SELECT *,\n" +
                "    ROW_NUMBER() OVER (PARTITION BY TUMBLE(ptm, INTERVAL '20' SECOND) ORDER BY price DESC) as rk \n" +
                "    FROM mock_order\n" +
                ")\n" +
                "WHERE rk = 1" ;


        System.out.println( excute_sql);

        TableResult tableResult = tableEnv.executeSql(excute_sql);
        tableResult.print();



    }
}
