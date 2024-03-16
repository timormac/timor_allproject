package demos;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Title: A2_开户需求
 * @Package: demos
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/13 20:55
 * @Version:1.0
 */
public class A2_开户需求 {
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
                "data['price'] as price  " +
                " from kafka_maxwell" +
                " where `table` = 'mock_order' ");
        tableEnv.createTemporaryView("mock_order",mock_order);

        /**需求说明:
         * 如果同一个spuid的创建时间相差超过30s,那么新增一个消息
         *
         *
         */



        //这个目前放弃，方式不对
//        //TODO 每个会话窗口关闭是时,按spuid分组,只返回一条数据
//        String excute_sql = "select \n" +
//                "    SESSION_START(PROCTIME(), INTERVAL '30' second) as w_start, \n" +
//                "    spuid, \n" +
//                "    first_value(orderno) as orderno, \n" +
//                "    first_value(price) as price, \n" +
//                "    first_value(create_time) as create_time\n" +
//                "from mock_order \n" +
//                "group by SESSION(PROCTIME(), INTERVAL '30' second), spuid\n" ;



        //这个sq，每次断开就会输出一条
        String excute_sql ="select \n" +
                "    SESSION_START(proctime(), interval '20' second) as w_start, \n" +
                "    SESSION_END(proctime(), interval '20' second) as w_end, \n" +
                "    spuid, \n" +
                "    count(*) as num, \n " +
                "    max(orderno) as orderno,\n" +
                "    max(price) as price, \n" +
                "    max(create_time) as create_time\n" +
                "from mock_order \n" +
                "group by SESSION(proctime(), interval '20' second), spuid\n";
        System.out.println(excute_sql);

        TableResult tableResult = tableEnv.executeSql(excute_sql);
        tableResult.print();


    }
}
