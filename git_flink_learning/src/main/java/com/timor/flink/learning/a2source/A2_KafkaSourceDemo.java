package com.timor.flink.learning.a2source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Title: A2_KafkaSourceDemo
 * @Package: com.timor.flink.learning.source
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/29 09:32
 * @Version:1.0
 */
public class A2_KafkaSourceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();

        //这些配置在官网能查到：https://kafka.apache.org/28/documentation.html#streamsconfigs
        properties.setProperty("bootstrap.servers", "project1:9092,project2:9092,project3:9092");
        properties.setProperty("group.id", "my_group1");
        //flink的offset和kafka的不同，earlist一定从最早，latest一定从最新，不管有没有offset
        properties.setProperty("auto.offset.reset", "none"); // 禁用自动重置偏移量

        //用property创建kafkasouce，具体能property能配置哪些参数看kafka官网
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setProperties(properties).build();


        //从kafka获取
//        KafkaSource<String> kafkaSource = KafkaSource.<String>builder() //builder前面要指定范型方法
//                .setBootstrapServers("project1:9092,project2:9092,project3:9092")
//                .setGroupId("aaa")
//                .setTopics("flink_word_topic")
//                .setValueOnlyDeserializer(new SimpleStringSchema())//指定从kafka获取的数据的反序列化器
//                .setStartingOffsets(OffsetsInitializer.latest())  //设置消费起始offset
//                .build();



        //flink的offset和kafka的不同，earlist一定从最早，latest一定从最新，不管有没有offset

        DataStreamSource<String> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource");

        ds.print();

        env.execute();
    }
}
