package com.utils;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

/**
 * @Title: KafkaConnector
 * @Package: com.lpc.kafka.tool
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/21 19:19
 * @Version:1.0
 */
public class A2_KafkaUtils {
    private static KafkaConsumer  consumer;
    private static KafkaProducer producer;


    public A2_KafkaUtils() { }

    public <K, V> KafkaConsumer<K, V> getConsumer(String groupId, String bootStrapServer, String keyDeSerialize, String valueDeSerialize){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer );
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeSerialize);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,valueDeSerialize);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        this.consumer = new KafkaConsumer<K, V>(properties);
        return   consumer ;
    }




    public static <K,V> KafkaProducer<K,V> getProducer( String keyClassName ,String valueClassName ){
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, A1_Config.KAFKA_SERVERS );

        //指定key序列化类型,  StringSerializer这个是kafka提供的String序列化器
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //指定value的序列化类型
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
        return  producer;

    }
    public static <K,V>   KafkaProducer<K,V> getProducer(){
        String name = StringSerializer.class.getName();
        return   getProducer(name,name);
    }







}
