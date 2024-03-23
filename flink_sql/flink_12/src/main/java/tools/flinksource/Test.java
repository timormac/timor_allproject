package tools.flinksource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Title: Test
 * @Package: tools.flinksource
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/23 02:46
 * @Version:1.0
 */
public class Test {
    public static void main(String[] args) throws Exception {

        Flink_KafkaSourceFactory.KafkaSource kafkaSource = Flink_KafkaSourceFactory.getKafkaSource();
        Flink_KafkaSourceFactory.GroupidKafkaSource groupidKafkaSource = kafkaSource.
                setTopicAndTable("maxwell", "tb1")
                .setServers("localhost:9092")
                .setGroupid("lpc");

        String connectsql = groupidKafkaSource
                .build()
                .setColumn("data", "map<string,string>")
                .setColumn("table", "string")
                .getConnectSql();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        System.out.println(connectsql);
        tableEnv.executeSql(connectsql);

        String excute_sql = "select " +
                            "data['orderno'] as orderno , " +
                            "data['spuid'] as spuid "+
                            " from tb1" ;

        TableResult tableResult = tableEnv.executeSql(excute_sql);
        tableResult.print();

    }
}
