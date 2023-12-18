package com.lpc.realtime.warehouse;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lpc.datamock.dao.Dao;
import com.lpc.datamock.dao.FlinkConfig;
import com.lpc.datamock.tools.A1_JDBCPool;
import com.lpc.datamock.tools.A2_TableQueryDao;
import com.lpc.realtime.warehouse.config.ConfigProperty;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * @Title: FlinkTestMain
 * @Package: com.lpc.realtime.warehouse
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/8 12:52
 * @Version:1.0
 */
public class FlinkTestMain {
    public static void main(String[] args) throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //从kafka获取
        KafkaSource<String> kf= KafkaSource.<String>builder() //builder前面要指定范型方法
                .setBootstrapServers(ConfigProperty.KAFKA_SERVER)
                .setGroupId("dim_consumer")
                .setTopics("maxwell")
                .setValueOnlyDeserializer(new SimpleStringSchema())//指定从kafka获取的数据的反序列化器
                .setStartingOffsets(OffsetsInitializer.latest())  //设置消费起始offset
                .build();

        DataStreamSource<String> kafkaDS = env.fromSource(kf, WatermarkStrategy.noWatermarks(), "kafkasource");

        SingleOutputStreamOperator<String> map = kafkaDS.map(s -> {
            JSONObject jsonObject = JSON.parseObject(s);
            String table = jsonObject.getString("table");
            String id = jsonObject.getString("id");
            return table + ":" + id;
        });

        kafkaDS.process(new ProcessFunction<String, String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx,
                                       Collector<String> out) throws Exception {

                            }
        });

    }
}
