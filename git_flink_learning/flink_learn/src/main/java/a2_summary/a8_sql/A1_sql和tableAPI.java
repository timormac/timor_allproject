package a2_summary.a8_sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author Timor
 * @Date 2024/2/28 22:57
 * @Version 1.0
 */
public class A1_sql和tableAPI {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("CREATE TABLE source ( id String) WITH('connector' = 'datagen')" );


        Table table = tableEnv.sqlQuery("select id,sum(vc) as sumVC from source where id>5 group by id ;");

        //TODO 和上面这个sql操作相同，这个是tableAPI
        Table source = tableEnv.from("source");
        Table result = source
                .where($("id").isGreater(5))
                .groupBy($("id"))
                .aggregate($("vc").sum().as("sumVC"))
                .select($("id"), $("sumVC"));

        tableEnv.createTemporaryView("tmp", table);



    }
}
