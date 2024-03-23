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
public class A2_窗口和lastvalue一起的效果 {
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

        //TODO 结果展示 窗口关后闭时，获取最后一条数据了,也就是说每个窗口只返回一条数据
        String excute_sql = "SELECT \n" +
                "mobile_tel,\n" +
                "user_id,\n" +
                "last_value(last_update_detetime) as last_update_detetime,\n" +
                "last_value(client_name) as client_name,\n" +
                "last_value(business_flag_last)  as business_flag_last,\n" +
                "TUMBLE_START(PROCTIME(), INTERVAL '20' SECOND) as ws_start,\n" +
                "TUMBLE_END(PROCTIME(), INTERVAL '20' SECOND) as ws_end\n" +
                "from rt_crhkh_crh_wskh_userqueryextinfo\n" +
                "group by TUMBLE(PROCTIME(), INTERVAL '20' SECOND),mobile_tel,user_id";


        System.out.println( excute_sql);

        TableResult tableResult = tableEnv.executeSql(excute_sql);

        tableResult.print();

    }
}
