package com.a1_kafkaAPI;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * @Title: A7_ConsumerOffset
 * @Package: com.a1_kafkaAPI
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/29 14:55
 * @Version:1.0
 */
public class A7_ConsumerOffset {
    public static void main(String[] args) {
        Properties properties = new Properties();

        //自动提交，默认是true
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"");
        //设置自动提交的间隔,默认是5s提交一次
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"");

        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Lists.newArrayList("topic1"));

        //TODO ----------------指定从offset开始消费----------------------
        /**  消费者组的分区安排需要时间,如果正在执行分配规则，kafkaConsumer.assignment()可能获取的是null
         *   源码里注释写了这种情况，通过while判断size保证成功后，再跳出
         *   源码的注释:gpt翻译如下
         *   如果消费者是通过直接分配分区（使用assign(Collection)方法）进行订阅的，那么该方法将返回与分配相同的分区集合。
         *   如果消费者是通过主题订阅进行订阅的，那么该方法将返回当前分配给消费者的主题分区集合。
         *   如果尚未进行分配，或者分区正在重新分配的过程中，可能返回空集合。
         */
        //返回当前消费者正在消费的所有分区集合
        Set<TopicPartition> assignment = kafkaConsumer.assignment();

        //保证分区已完成
        while ( assignment.size() ==0 ){
            kafkaConsumer.poll(Duration.ofSeconds(1));
            assignment = kafkaConsumer.assignment();
        }

        //设置所有的topic的partition都从100开始消费
        for (TopicPartition topicPartition : assignment) {
            kafkaConsumer.seek(topicPartition,100);
        }

        //TODO ----------------指定从时间点开始消费----------------------
        //返回当前消费者正在消费的所有分区集合
        Set<TopicPartition> ass2 = kafkaConsumer.assignment();
        //保证分区已完成
        while ( assignment.size() ==0 ){
            kafkaConsumer.poll(Duration.ofSeconds(1));
            assignment = kafkaConsumer.assignment();
        }

        HashMap<TopicPartition, Long> map2 = new HashMap<>();
        //获取当前消费者有哪些分区,并传入想要开始的时间到map中
        for (TopicPartition partiton : ass2) {
            map2.put(partiton,1231111L);
        }

        //这个方法会获取map中所有分区,返回你传入的时间的offset
        Map<TopicPartition, OffsetAndTimestamp> timeMap = kafkaConsumer.offsetsForTimes(map2);

        //每个分区指定offset，与前面指定offset消费相同，多了个转化的步骤
        for (TopicPartition part : ass2) {
            long offset = timeMap.get(part).offset();
            kafkaConsumer.seek(part,offset);
        }

        //TODO ----------------消费手动提交offset----------------------
        while (true){
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : poll) {
                System.out.println(record);
                //获取偏移量，提交时用
                record.offset();
            }


            /**
             * 同步提交会阻塞
             * commitSync：无参数，默认是提交上一次拉取的batch的offset
             * commitSync(Duration) ,设置一个超时时间
             * commitSync(Map< TopicPartition, OffsetAndMetadata > map) ,提交指定offset
             * 异步不会阻塞
             * commitAsync
             */

            //无参数默认提交上一次拉取的batch的offset
            kafkaConsumer.commitSync();
            //设置超时时间
            kafkaConsumer.commitSync(Duration.ofSeconds(5));
            //指定offset偏移量提交，代码应放在batch循环里
            TopicPartition t1= new TopicPartition("t1", 0);
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(100L);
            HashMap<TopicPartition, OffsetAndMetadata> map = Maps.newHashMap();
            map.put(t1,offsetAndMetadata);
            kafkaConsumer.commitSync(map);

            //异步提交
            kafkaConsumer.commitAsync();

        }



    }
}
