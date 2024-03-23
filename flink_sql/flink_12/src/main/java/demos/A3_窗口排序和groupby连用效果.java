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
public class A3_窗口排序和groupby连用效果 {

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


        String sink_stream_crhkh_crh_wskh_mid_sql = "create temporary view rt_crhkh_crh_wskh_userqueryextinfo as " +
                "select " +
                "data['user_id'] as user_id , " +
                "data['mobile_tel'] as mobile_tel ,"+
                "data['branch_no'] as branch_no , "+
                "data['business_flag_last'] as business_flag_last , " +
                "data['request_status'] as request_status  ," +
                "data['submit_datetime'] as submit_datetime , " +
                "data['channel_code'] as channel_code , " +
                "data['last_update_detetime'] as last_update_detetime , " +
                "data['client_name'] as client_name , " +
                "data['id_no'] as id_no , " +
                "data['birthday'] as birthday  ," +

                "CURRENT_TIMESTAMP as  ddd_timestamp," +
                "CURRENT_TIME as ddd_time ," +
                "CURRENT_DATE as ddd_date ," +

                "data['request_no'] as request_no  " +
                " from kafka_maxwell" +
                " where `table` = 'rt_crhkh_crh_wskh_userqueryextinfo' ";

        //System.out.println(sink_stream_crhkh_crh_wskh_mid_sql);

        //创建中间视图
        tableEnv.executeSql(sink_stream_crhkh_crh_wskh_mid_sql);


        //TODO group by字段 如果没有last_update_detetime,那么开窗中用到了last_update_detetime,会报错：'last_update_detetime' is not being grouped
//        String excute_sql = "select \n" +
//                "user_id,\n" +
//                "mobile_tel,\n" +
//                "client_name,\n" +
//                "row_number()over( partition by  user_id,mobile_tel  order by last_update_detetime ) as rk\n" +
//                "from rt_crhkh_crh_wskh_userqueryextinfo\n" +
//                "group by user_id,mobile_tel,client_name";

//
//        String excute_sql="SELECT \n" +
//                "user_id,\n" +
//                "mobile_tel,\n" +
//                "last_update_detetime ,\n" +
//                "ROW_NUMBER() OVER(PARTITION BY user_id, mobile_tel order by last_update_detetime) rn\n" +
//                "from rt_crhkh_crh_wskh_userqueryextinfo\n" +
//                "group by user_id,mobile_tel,last_update_detetime";


        String excute_sql="select \n" +
                "*\n" +
                "from (\n" +
                "\n" +
                "    SELECT \n" +
                "    user_id,\n" +
                "    mobile_tel,\n" +
                "    last_update_detetime ,\n" +
                "    ROW_NUMBER() OVER(PARTITION BY user_id, mobile_tel order by last_update_detetime) rn\n" +
                "    from rt_crhkh_crh_wskh_userqueryextinfo \n" +
                "    group by user_id,mobile_tel,last_update_detetime\n" +
                ")\n" +
                "where rn = 1";

        System.out.println( excute_sql);

        TableResult tableResult = tableEnv.executeSql(excute_sql);

        tableResult.print();



    }
}
