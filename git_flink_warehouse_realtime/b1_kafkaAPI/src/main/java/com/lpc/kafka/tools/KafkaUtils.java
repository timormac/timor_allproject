package com.lpc.kafka.tools;

import com.lpc.datamock.tools.A0_PropertyUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

/**
 * @Title: KafkaConnector
 * @Package: com.lpc.kafka.tool
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/21 19:19
 * @Version:1.0
 */
public class KafkaUtils <K,V>{
    private KafkaConsumer consumer;
    private KafkaProducer<K,V> producer;
    private Properties property;
    private Properties consumer_property;
    private Properties producer_property;

    //加载properties文件
    {
        try {
            this.property = new A0_PropertyUtils("kafka_config.properties").getProperty();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public KafkaUtils() { }

    public void  consumerSetProperty(String key,String value){
        this.consumer_property.setProperty(key,value);
        this.consumer =  new KafkaConsumer(consumer_property);
    }

    public void  producerSetProperty(String key,String value){
        this.producer_property.setProperty(key,value);
        this.producer = new KafkaProducer<>(this.producer_property);
    }

    public KafkaConsumer getConsumer(String groupId, String bootStrapServer, String keyDeSerialize, String valueDeSerialize){
        this.consumer_property = new Properties();
        consumer_property.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer );
        consumer_property.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeSerialize);
        consumer_property.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,valueDeSerialize);
        consumer_property.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        this.consumer = new KafkaConsumer<K, V>(consumer_property);
        return   consumer ;
    }

    public KafkaConsumer getConsumer(String groupId, String bootStrapServer){
        String name = StringDeserializer.class.getName();
        return  getConsumer(groupId,bootStrapServer,name,name );
}


    public KafkaConsumer getConsumer(String groupId){
        String name = StringDeserializer.class.getName();
       return  getConsumer(groupId ,property.getProperty("bootstrap_servers"),name,name );
    }

    public KafkaProducer<K,V> getProducer(){
        this.producer_property = new Properties();
        String name = StringSerializer.class.getName();
        producer_property.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, property.getProperty("bootstrap_servers") );
        producer_property.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, name);
        producer_property.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,name);
        this.producer = new KafkaProducer<>(this.producer_property);
        return  this.producer;

    }



}
