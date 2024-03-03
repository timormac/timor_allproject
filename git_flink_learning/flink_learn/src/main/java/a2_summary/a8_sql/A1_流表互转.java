package a2_summary.a8_sql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author Timor
 * @Date 2024/2/28 17:22
 * @Version 1.0
 */
public class A1_流表互转 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Integer> sensorDS = env.fromElements(1,2);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 1. 流转表
        Table sensorTable = tableEnv.fromDataStream(sensorDS);
        tableEnv.createTemporaryView("sensor", sensorTable);

        //查询时根据流的类的变量名调用
        Table filterTable = tableEnv.sqlQuery("select id,ts,vc from sensor where ts>2");
        Table sumTable = tableEnv.sqlQuery("select id,sum(vc) from sensor group by id");


        // TODO 2. 表转流
        // 2.1 追加流
        tableEnv.toDataStream(filterTable, Integer.class).print("filter");
        // 2.2 changelog流(结果需要更新),更新流里,有特殊数据，你要特殊处理
        tableEnv.toChangelogStream(sumTable ).print("sum");


        // 只要代码中调用了 DataStreamAPI，就需要 execute，否则不需要
        env.execute();



    }
}
