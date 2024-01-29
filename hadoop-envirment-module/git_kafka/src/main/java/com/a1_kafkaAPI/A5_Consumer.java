package com.a1_kafkaAPI;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @Title: A5_Consumer
 * @Package: com.a1_kafkaAPI
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/26 16:33
 * @Version:1.0
 */
public class A5_Consumer {
    public static void main(String[] args) {
        Properties properties = new Properties();

        //server
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"");
        //序列化/反序列化器
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //groupid
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"");
        //设置消费者分区规则
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,"");

        //自动提交，默认是true
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"");
        //设置自动提交的间隔,默认是5s提交一次
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"");



        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties);

        /**
         * 订阅主题,这种情况一般是自己使用
         */
        ArrayList<String> list = new ArrayList<>();
        list.add("topic");
        //订阅主题,传一个Collctions,若只订阅一个主题,就只放一个元素就行
        kafkaConsumer.subscribe(list);
        while (true){
            //手动设定拉取频率一次拉取的是一个batch的所有数据
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : poll) {
                System.out.println(record);
            }

        }


    }
}
