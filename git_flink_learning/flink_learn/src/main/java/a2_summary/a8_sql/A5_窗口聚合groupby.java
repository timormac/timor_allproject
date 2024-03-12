package a2_summary.a8_sql;

import a2_summary.a8_sql.tools.A0_SQLConnect;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author Timor
 * @Date 2024/3/11 5:20
 * @Version 1.0
 */
public class A5_窗口聚合groupby {
    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String kf_conn_sql = "CREATE TABLE kafka_maxwell( \n" +
                "`database` string, " +
                "`table` string, " +
                "`data` map<string, string>, " +
                "`type` string, " +
                "`ts` string "+ A0_SQLConnect.kafkaSourceDDL("maxwell","lpc");

        tableEnv.executeSql(kf_conn_sql);


        tableEnv.sqlQuery()

    }
}
