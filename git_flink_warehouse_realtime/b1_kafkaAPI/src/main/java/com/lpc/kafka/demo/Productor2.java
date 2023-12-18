package com.lpc.kafka.demo;

import com.lpc.datamock.tools.A0_PropertyUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

/**
 * @Author Timor
 * @Date 2023/10/21 20:37
 * @Version 1.0
 */
public class Productor2 {
    public static void main(String[] args) throws IOException {

        Properties file = new A0_PropertyUtils("kafka_config.properties").getProperty();
        Properties properties = new Properties();

        //使用这种枚举的方式获取参数
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, file.getProperty("bootstrap_servers") );
        //指定key的序列化类型
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //指定key的序列化类型
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);


        String topic = "terminal_produce";

        //默认是异步发送
        for (int i = 0; i < 10; i++) {

            kafkaProducer.send( new ProducerRecord<>(topic,"bbb"));
        }

        kafkaProducer.close();




    }
}
