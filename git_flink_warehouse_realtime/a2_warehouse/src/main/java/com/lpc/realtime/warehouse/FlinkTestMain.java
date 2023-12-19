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

        String id = "haha" ;
        String s="{\"address\":\"北京市\",\"age\":20,\"name\":\"张三\"" +
                ",\"id\":"+ "\""+id +"\"" +
                "}";

        System.out.println(s);

        JSONObject jsonObject = JSON.parseObject(s);
        System.out.println(jsonObject.getString("id"));



    }
}
