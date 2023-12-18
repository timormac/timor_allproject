package com.lpc.kafka;

import com.lpc.kafka.tools.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @Author Timor
 * @Date 2023/10/21 21:39
 * @Version 1.0
 */
public class Test2 {
    public static void main(String[] args) {

        KafkaUtils<String, String> util = new KafkaUtils<>();
        KafkaConsumer consumer = util.getConsumer("group_asd");
        consumer.subscribe(Arrays.asList("terminal_produce"));
        //5. 消费数据
        while (true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord :
                    consumerRecords) {
                System.out.println(consumerRecord.value());
            }
        }


    }



}
