package com.lpc.kafka;

import com.lpc.kafka.tools.KafkaUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.TransferQueue;

/**
 * @Author Timor
 * @Date 2023/10/21 21:32
 * @Version 1.0
 */
public class Test {

    public static void main(String[] args) throws InterruptedException {

        KafkaUtils<String, String> util = new KafkaUtils<>();
        KafkaProducer<String, String> producer = util.getProducer();

        while (true){
            producer.send( new ProducerRecord<>("terminal_produce","888"));
            Thread.sleep(2000L);
        }

    }
}
