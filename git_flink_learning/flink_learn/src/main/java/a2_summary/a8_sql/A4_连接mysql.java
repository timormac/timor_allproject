package a2_summary.a8_sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author Timor
 * @Date 2024/2/28 13:42
 * @Version 1.0
 */
public class A4_连接mysql {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String  mysql_conn = "CREATE TABLE mock_cupon\n" +
                "(\n" +
                "  id bigint,\n" +
                "  cuponno string,\n" +
                "  orderno string,\n" +
                "  activity string\n" +
                ")WITH (\n" +
                "    'connector'='jdbc',\n" +
                "    'url' = 'jdbc:mysql://project1:3306/flink_warehouse_db?useUnicode=true&characterEncoding=utf8',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '121995',\n" +
                "    'connection.max-retry-timeout' = '60s',\n" +
                "    'table-name' = 'mock_cupon'\n" +
                ");\n";
        tableEnv.executeSql(mysql_conn);

        //TODO 读取的是mysql表的全部数据,并不是cdc数据
        TableResult tableResult = tableEnv.executeSql("select * from mock_cupon");
        tableResult.print();


    }
}
