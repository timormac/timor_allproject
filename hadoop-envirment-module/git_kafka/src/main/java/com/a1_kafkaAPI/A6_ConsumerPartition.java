package com.a1_kafkaAPI;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @Title: ConsumerPartition
 * @Package: com.a1_kafkaAPI
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/29 13:56
 * @Version:1.0
 */
public class A6_ConsumerPartition {

    public static void main(String[] args) {
        Properties properties = new Properties();
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties);

        /**
         * 订阅分区,这种情况一般框架使用，比如flink的checkpoint恢复时，会使用订阅分区
         */
        ArrayList<TopicPartition> partition = new ArrayList<>();
        partition.add( new TopicPartition("topicA",1)  );
        kafkaConsumer.assign(partition);
        while (true){

            ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : poll) {
                System.out.println(record);
            }

        }


    }
}
