package demand;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Title: A2_消费kafka数据查验
 * @Package: demand
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/16 09:09
 * @Version:1.0
 */
public class A2_消费kafka数据查验 {
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

        String temp_sql = "create temporary view rt_crhkh_crh_wskh_userqueryextinfo as " +
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

        //System.out.println(temp_sql);

        //创建中间视图
        tableEnv.executeSql(temp_sql);

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

        TableResult tableResult = tableEnv.executeSql("select * from rt_crhkh_crh_wskh_userqueryextinfo ");
        tableResult.print();


    }
}
