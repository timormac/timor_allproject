package com.lpc.realtime.warehouse.questions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lpc.hbase.tools.HbaseTools;
import com.lpc.realtime.warehouse.config.ConfigProperty;
import com.lpc.realtime.warehouse.utils.JsonTool;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * @Author Timor
 * @Date 2023/11/23 16:45
 * @Version 1.0
 */
public class A1_ConnectionReuse {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        //从kafka获取
        KafkaSource<String> kf= KafkaSource.<String>builder() //builder前面要指定范型方法
                .setBootstrapServers(ConfigProperty.KAFKA_SERVER)
                .setGroupId("dim_consumer")
                .setTopics("maxwell")
                .setValueOnlyDeserializer(new SimpleStringSchema())//指定从kafka获取的数据的反序列化器
                .setStartingOffsets(OffsetsInitializer.latest())  //设置消费起始offset
                .build();

        DataStreamSource<String> kafkaDS = env.fromSource(kf, WatermarkStrategy.noWatermarks(), "kafkasource");
        SingleOutputStreamOperator<JSONObject> jsDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                if (JsonTool.isJSON(value)) out.collect(JSON.parseObject(value));
            }
        });

        SingleOutputStreamOperator<JSONObject> filter = jsDS.filter(js -> "orderinfo".equals(js.getString("table")));


        //在富函数中的open方法里执行数据初始化连接没有问题
        //之前的getData代码在connect流中，怎么一次创建连接呢
        filter.addSink(new RichSinkFunction<JSONObject>() {
            Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum","project1,project2,project3");
                this.connection = ConnectionFactory.createConnection(config);
                System.out.println( "执行了一次open"  );
            }

            @Override
            public void invoke(JSONObject value, Context context) throws Exception {
                String tableName = "tb1";
                String columeName = "orderno";
                String rowkey = value.getJSONObject("data").getString("orderno");
                String v = value.getJSONObject("data").getString("orderno");
                System.out.println(v);
                HbaseTools.insertData(connection,tableName,columeName,rowkey,v,"cols1","dev");
            }

        });

        env.execute();


    }



}
