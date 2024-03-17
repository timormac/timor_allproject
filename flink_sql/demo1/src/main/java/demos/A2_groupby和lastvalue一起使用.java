package demos;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Title: A2_groupby和lastvalue一起使用
 * @Package: demos
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/17 07:42
 * @Version:1.0
 */
public class A2_groupby和lastvalue一起使用 {


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
                "data['request_no'] as request_no  " +
                " from kafka_maxwell" +
                " where `table` = 'rt_crhkh_crh_wskh_userqueryextinfo' ";

        //System.out.println(sink_stream_crhkh_crh_wskh_mid_sql);

        //创建中间视图
        tableEnv.executeSql(sink_stream_crhkh_crh_wskh_mid_sql);

        //TODO group by + last_value, 和max()差不多,每来一条数据,就会更新，而且是个回撤流
//        String excute_sql = "select \n" +
//                "mobile_tel,\n" +
//                "user_id,\n" +
//                "last_value(last_update_detetime) as last_update_detetime\n" +
//                "from rt_crhkh_crh_wskh_userqueryextinfo\n" +
//                "group by mobile_tel,user_id";

        //TODO first_value, 只输出一条,就是第一条来的数据,后序数据都被过滤掉了
        String excute_sql = "select \n" +
                "mobile_tel,\n" +
                "user_id,\n" +
                "first_value(last_update_detetime) as last_update_detetime\n" +
                "from rt_crhkh_crh_wskh_userqueryextinfo\n" +
                "group by mobile_tel,user_id";


        System.out.println( excute_sql);

        TableResult tableResult = tableEnv.executeSql(excute_sql);

        tableResult.print();


    }

}
