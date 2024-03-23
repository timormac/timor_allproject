package a2_易错点与bug;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import tools.flinksource.Flink_KafkaSourceFactory;


/**
 * @Title: A1_回撤流导致开窗函数只有1条数据
 * @Package: a2_易错点与bug
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/23 09:19
 * @Version:1.0
 */
public class A1_回撤流导致开窗函数只有1条数据 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String kf_conn = Flink_KafkaSourceFactory
                .getKafkaSource()
                .setTopicAndTable("stream_mot_stream_account_break_ths_cc_mid", "kafka_data")
                .setServers("localhost:9092")
                .setGroupid("lpc")
                .build()
                .setColumn("mobile", "string")
                .setColumn("occur_date", "string")
                .setColumn("occur_time", "string")
                .getConnectSql();

        System.out.println(kf_conn);

        tableEnv.executeSql(kf_conn);


//        String kf_conn = "CREATE TABLE kafka_data( \n" +
//                "`mobile` string ," +
//                "`occur_date` string ," +
//                "`occur_time` string " +
//                ")WITH (\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
//                "  'topic' = 'stream_mot_stream_account_break_ths_cc_mid',\n" +
//                "  'properties.group.id' = 'lpc',\n" +
//                "  'scan.startup.mode' = 'latest-offset',\n" +
//                "  'format' = 'json'\n" +
//                ") \n";





        //TODO 有回撤流
        String test_sql =   "select \n" +
                "last_value(mobile) as mobile ,\n" +
                "last_value(occur_date) as occur_date ,\n" +
                "last_value(occur_time) as occur_time \n" +
                "from  kafka_data\n" +
                "group by mobile,occur_date";

        /*
| op |                         mobile |                     occur_date |                     occur_time |
+----+--------------------------------+--------------------------------+--------------------------------+
| +I |                   112778845566 |                       20240322 |                       13:25:45 |
| -U |                   112778845566 |                       20240322 |                       13:25:45 |
| +U |                   112778845566 |                       20240322 |                       13:25:48 |
| -U |                   112778845566 |                       20240322 |                       13:25:48 |
| +U |                   112778845566 |                       20240322 |                       13:25:50 |
         */


        //TODO 这个写法,有个回撤流,会导致group by的那个sql,当更新时，会删除原来的数据,
        // 导致一直只有一条数据,所以新来的数据就算occur_time大,也是rk等于1

//        String  test_sql = "select \n" +
//                "*\n" +
//                "from (\n" +
//                "\tselect \n" +
//                "\tmobile,\n" +
//                "\toccur_date,\n" +
//                "\toccur_time,\n" +
//                "\trow_number()over(partition by occur_date,mobile order by occur_time) as rk\n" +
//                "\tfrom (\n" +
//                "\n" +
//                "\t\tselect \n" +
//                "\t\tmobile,\n" +
//                "\t\toccur_date ,\n" +
//                "\t\tlast_value(occur_time) as occur_time \n" +
//                "\t\tfrom  kafka_data\n" +
//                "\t\tgroup by mobile,occur_date\n" +
//                "\t)\n" +
//                ")\n" +
//                "where rk = 1";
/*
| op |                         mobile |                     occur_date |                     occur_time |                   rk |
+----+--------------------------------+--------------------------------+--------------------------------+----------------------+
| +I |                   112778845566 |                       20240322 |                       13:03:07 |                    1 |
| -D |                   112778845566 |                       20240322 |                       13:03:07 |                    1 |
| +I |                   112778845566 |                       20240322 |                       13:03:09 |                    1 |
| -D |                   112778845566 |                       20240322 |                       13:03:09 |                    1 |
| +I |                   112778845566 |                       20240322 |                       13:03:11 |                    1 |
 */

        //TODO 这个写法就行了
//        String  test_sql =  "select \n" +
//                "*\n" +
//                "from (\n" +
//                "\tselect \n" +
//                "\tmobile,\n" +
//                "\toccur_date,\n" +
//                "\toccur_time,\n" +
//                "\trow_number()over(partition by occur_date,mobile order by occur_time) as rk\n" +
//                "\tfrom kafka_data\n" +
//                ")\n" +
//                "where rk = 1\n";


        TableResult tableResult = tableEnv.executeSql(test_sql);
        tableResult.print();


    }
}
