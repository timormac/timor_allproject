package com.lpc.kafka.demo;

import com.lpc.datamock.tools.A0_PropertyUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

/**
 * @Author Timor
 * @Date 2023/10/21 20:48
 * @Version 1.0
 */
public class producer4 {
    public static void main(String[] args) throws IOException {


        //这个代码会卡住了不能执行，不知道为什么？？？

        Properties file = new A0_PropertyUtils("kafka_config.properties").getProperty();
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, file.getProperty("bootstrap_servers") );
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);
        String topic = "terminal_produce";


        long stmap = System.currentTimeMillis()-10;

        //指定topic,分区,时间戳(null默认系统时间,key,value,header不知道干什么用的
        ProducerRecord<String, String> record2 = new ProducerRecord<String, String>(
                topic,1, stmap,"key","value",null);

        kafkaProducer.send(record2);

        kafkaProducer.close();



    }
}
