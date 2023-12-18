package com.lpc.kafka.demo;

import com.lpc.kafka.tools.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * @Title: UdfConsumerAssignTime
 * @Package: com.lpc.kafka.demo
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/21 19:18
 * @Version:1.0
 */
public class UdfConsumerAssignTime {
    public static void main(String[] args) {

        KafkaUtils<String,String> kafkaUtils = new KafkaUtils<>();
        KafkaConsumer<String, String> kafkaConsumer = kafkaUtils.getConsumer("groupid_time");
        ArrayList<String> topics = new ArrayList<>();
        topics.add("terminal_produce");
        kafkaConsumer.subscribe(topics);

        Set<TopicPartition> assignment = new HashSet<>();

        // 获取消费者分区分配信息(有了分区分配信息才能开始消费) assignment = kafkaConsumer.assignment();
        while (assignment.size() == 0) {
            kafkaConsumer.poll(Duration.ofSeconds(1));
        }

        HashMap<TopicPartition, Long> timestampToSearch = new HashMap<>();
        // 封装集合存储，每个分区对应一天前的数据
        for (TopicPartition topicPartition : assignment) {
            timestampToSearch.put(topicPartition, System.currentTimeMillis() - 1 * 24 * 3600 * 1000);
        }

        // 获取从1天前开始消费的每个分区的offset
        Map<TopicPartition, OffsetAndTimestamp> offsets = kafkaConsumer.offsetsForTimes(timestampToSearch);

        // 遍历每个分区，对每个分区设置消费时间。
        for (TopicPartition topicPartition : assignment) {
            OffsetAndTimestamp offsetAndTimestamp = offsets.get(topicPartition);
            // 根据时间指定开始消费的位置
            if (offsetAndTimestamp != null){
                kafkaConsumer.seek(topicPartition, offsetAndTimestamp.offset());
            }
        }

        while(true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1L));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record );
            }
        }


    }



}
