package a2_summary.a8_sql;

import a2_summary.a8_sql.tools.a0_SQLConnect;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author Timor
 * @Date 2024/2/28 16:20
 * @Version 1.0
 */
public class A6_滚动窗口 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);



        String kf_conn = "create table topic_maxwell(\n" +
                "`database` string,\n" +
                "`table` string, \n " +
                "`data` map<string, string>, \n " +
                "`type` string,  \n" +
                "`ts` string  \n"+
                ")" + a0_SQLConnect.kafkaSourceDDL("maxwell","test");

        System.out.println(kf_conn);

        tableEnv.executeSql(kf_conn);


        tableEnv.executeSql("select " +
                "data['orderno'] as orderno" +
                "data['create_time'] as create_time"   +
                "from topic_maxwell" +
                "where `table`='mock_order'" +
                ")"
        );









    }
}
