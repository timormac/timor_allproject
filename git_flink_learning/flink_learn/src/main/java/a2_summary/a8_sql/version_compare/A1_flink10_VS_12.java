package a2_summary.a8_sql.version_compare;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Title: A1_flink10_VS_12
 * @Package: a2_summary.a8_sql.version_compare
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/17 06:38
 * @Version:1.0
 */
public class A1_flink10_VS_12 {
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


        //TODO  开窗函数partition的字段,必须全部select出来, select后 rk<3 可以执行
        //TODO  开窗函数partition的字段,没全部select出来,只能执行rk =1 活着rk=3进行等号操作
        //TODO  开窗函数partition的字段,没全部select出来,执行rk<3 会报错,#Field names must be unique. Found duplicates: [$3]

        //TODO 在flink1.17中就没问题,解决了这个bug

        String excute_sql = "select  \n" +
                "mobile_tel, \n" +
                "business_flag_last, \n" +
                "last_update_detetime, \n" +
                "rn \n" +
                "from( \n" +
                "    SELECT \n" +
                "    mobile_tel, \n" +
                "    business_flag_last, \n" +
                "    last_update_detetime, \n" +
                "    ROW_NUMBER() OVER(PARTITION BY mobile_tel,business_flag_last,channel_code,to_date(last_update_detetime) order by last_update_detetime) rn \n" +
                "    from rt_crhkh_crh_wskh_userqueryextinfo \n" +
                ") t \n" +
                "where t.rn < 3";

        System.out.println( excute_sql);

        TableResult tableResult = tableEnv.executeSql(excute_sql);

        tableResult.print();




    }
}
