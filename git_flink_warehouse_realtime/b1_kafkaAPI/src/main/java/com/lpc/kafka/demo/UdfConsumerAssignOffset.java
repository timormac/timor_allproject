package com.lpc.kafka.demo;

import com.lpc.kafka.tools.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * @Title: UdfConsumerAssignOffset
 * @Package: com.lpc.kafka.demo
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/21 19:18
 * @Version:1.0
 */
public class UdfConsumerAssignOffset {
    public static void main(String[] args) {
        KafkaUtils<String,String> kafkaUtils = new KafkaUtils<>();
        KafkaConsumer<String, String> kafkaConsumer = kafkaUtils.getConsumer("grouid_offset");
        ArrayList<String> topics = new ArrayList<>();
        topics.add("terminal_produce");
        kafkaConsumer.subscribe(topics);
        Set<TopicPartition> assignment= new HashSet<>();

        // 获取消费者分区分配信息(有了分区分配信息才能开始消费) assignment = kafkaConsumer.assignment();
        while (assignment.size() == 0) {
            kafkaConsumer.poll(Duration.ofSeconds(1));
        }
        // 遍历所有分区，并指定 offset 从 1700 的位置开始消费
        for (TopicPartition tp: assignment) {
            kafkaConsumer.seek(tp, 1700);
        }

        while(true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1L));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record );
            }
        }

    }
}
