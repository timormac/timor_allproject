package com.lpc.realtime.warehouse.a7_app.dim;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lpc.datamock.dao.Dao;
import com.lpc.datamock.dao.FlinkConfig;
import com.lpc.datamock.tools.A1_JDBCPool;
import com.lpc.datamock.tools.A2_TableQueryDao;
import com.lpc.hbase.tools.HbaseTools;
import com.lpc.realtime.warehouse.a3_dao.PojoString;
import com.lpc.realtime.warehouse.a2_utils.A1_ConfigProperty;
import com.lpc.realtime.warehouse.a2_utils.Tm_JsonTool;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.client.Connection;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * @Author Timor
 * @Date 2023/11/22 15:37
 * @Version 1.0
 */
public class A1_GetDataOptimize {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        //从kafka获取
        KafkaSource<String> kf= KafkaSource.<String>builder() //builder前面要指定范型方法
                .setBootstrapServers(A1_ConfigProperty.KAFKA_SERVER)
                .setGroupId("dim_consumer")
                .setTopics("maxwell")
                .setValueOnlyDeserializer(new SimpleStringSchema())//指定从kafka获取的数据的反序列化器
                .setStartingOffsets(OffsetsInitializer.latest())  //设置消费起始offset
                .build();

        DataStreamSource<String> kafkaDS = env.fromSource(kf, WatermarkStrategy.noWatermarks(), "kafkasource");


        DruidPooledConnection poolConnect = A1_JDBCPool.getPoolConnect();
        A2_TableQueryDao query = new A2_TableQueryDao(poolConnect);
        ArrayList<Dao> querySet = query.DaoQueryset("flinkconfig", "com.lpc.datamock.dao.FlinkConfig");
        query.close();

        //获取mysql初始化列表
        HashSet<String> set = new HashSet<>();
        for (Dao dao : querySet) {
            String table_name = ((FlinkConfig) dao).getTable_name();
            set.add(table_name);
        }

        OutputTag wrongJson = new OutputTag("wrongJson", Types.POJO(PojoString.class));
        OutputTag config = new OutputTag("config",Types.POJO(PojoString.class));

        //分流
        SingleOutputStreamOperator<String> mainDS = kafkaDS.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                //非JSON,分流到侧输出流wrongJson
                if (!Tm_JsonTool.isJSON(value)) ctx.output(wrongJson, new PojoString(value) );
                    //监控config表分流到config
                else if ("flinkconfig".equals(JSON.parseObject(value).getString("table"))) {
                    JSONObject data = JSON.parseObject(value).getJSONObject("data");
                    String newTable = data.getString("table_name");
                    ctx.output(config,new PojoString(newTable));
                    System.out.println("配置流中增加"+newTable);

                }
                //其他json数据放入到主流
                else{
                    out.collect(value);
                    String table = JSON.parseObject(value).getString("table");
                    //System.out.println( "主流里来了条数据"+ table);
                }
            }
        });


        //侧输出流获取异常JSON数据
        SideOutputDataStream wrongJsonOutput = mainDS.getSideOutput(wrongJson);

        //获取配置流并广播
        SideOutputDataStream configOutput = mainDS.getSideOutput(config);
        DataStream<PojoString> configBroadcast = configOutput.broadcast();

        SingleOutputStreamOperator<JSONObject> mainDSJson = mainDS.map(JSON::parseObject);

        ConnectedStreams<JSONObject, PojoString> connect = mainDSJson.connect(configBroadcast);


        //创建hbase连结
        SingleOutputStreamOperator<JSONObject> needDimDs = connect.process(new CoProcessFunction<JSONObject, PojoString, JSONObject>() {
            HashSet<String> configSet = new HashSet<>();
            Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("初始化set集合");
                for (String s : set) {
                    configSet.add(s);
                }

                this.connection = HbaseTools.getHbaseConnection(A1_ConfigProperty.HBASE_SERVER);
                System.out.println("open中创建hbase连结");

            }

            @Override
            public void processElement1(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

                System.out.print("当前set集合数据为");
                for (String s : configSet) {
                    System.out.print(s + "//");
                }
                System.out.println("");
                String table = value.getString("table");
                if (configSet.contains(table)) {
                    out.collect(value);
                    System.out.println("收集一条数据" + table);
                } else System.out.println("过滤了一个不要数据" + table);

            }

            @Override
            public void processElement2(PojoString value, Context ctx, Collector<JSONObject> out) throws Exception {
                System.out.println("config表新增数据" + value.getValue());
                configSet.add(value.getValue());
                HbaseTools.createTable( connection,value.getValue() );
            }

        });


        //sink数据到hbase
        //本事只要实现sinkFunction就行,但是为了使用open创建连结,实现了RichSinkFunction
        needDimDs.addSink(new RichSinkFunction<JSONObject>() {
            Connection connection;
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("看看open执行几次");
                this.connection = HbaseTools.getHbaseConnection(A1_ConfigProperty.HBASE_SERVER);
            }

            @Override
            public void invoke(JSONObject value, Context context) throws Exception {
                System.out.println(value);
                String sink_table = value.getString("table");
                JSONObject dataJson = value.getJSONObject("data");
                String rowkey = dataJson.getString("id");
                HbaseTools.insertData(connection,sink_table,"id",rowkey,rowkey);

            }


        });


        wrongJsonOutput.print("测输出流错误数据");
        needDimDs.print();
        env.execute();




    }
}
