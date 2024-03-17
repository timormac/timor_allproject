package demos;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Title: TTTest
 * @Package: demos
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/17 08:50
 * @Version:1.0
 */
public class TTTest {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);



        String  mysql_conn = "CREATE TABLE flink_date\n" +
                "(\n" +
                "  id bigint,\n" +
                "  ddd_time string,\n" +
                "  ddd_date string,\n" +
                "  ddd_timestamp string,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED" +
                ")WITH (\n" +
                "    'connector'='jdbc',\n" +
                "    'url' = 'jdbc:mysql://localhost:3306/flink_warehouse_db?useUnicode=true&characterEncoding=utf8',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'lpc19950419',\n" +
                "    'table-name' = 'flink_date'\n" +
                ")\n";

        tableEnv.executeSql(mysql_conn);

        String excute_sql = "  insert into flink_date  select 1 , cast(CURRENT_TIME as string),cast(CURRENT_DATE as string),cast( CURRENT_TIMESTAMP as string)  " ;





//        String excute_sql = "  insert into flink_date  select 1,  cast(ddd_time as string),  cast(ddd_date as string), cast(ddd_timestamp as string)  from rt_crhkh_crh_wskh_userqueryextinfo ";



        System.out.println( excute_sql);

        TableResult tableResult = tableEnv.executeSql(excute_sql);

        tableResult.print();
    }
}
