package com.a1_kafkaAPI;

import com.utils.A1_Config;
import com.utils.A2_KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @Title: A2_AsynsProducer
 * @Package: com.a1_kafkaAPI
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/25 15:46
 * @Version:1.0
 */
public class A2_Producer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, A1_Config.KAFKA_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaProducer<String,String> producer =  new KafkaProducer<>(properties);
        //最全的参数:topi,borker序号,时间戳,key,value,header??
        ProducerRecord record0 = new ProducerRecord<>("topic",1,1223312313L,"key","value",null);
        ProducerRecord record = new ProducerRecord<>("topic","a");

        //异步发送
        producer.send(record);

        //异步+.get()就是同步发送
        producer.send(record).get();


        /**
         * 异步发送，就是直接执行发送操作，不管后续是否成功，继续下一行代码
         * 一般通过发送消息里面封装的回调函数来，处理发送失败后，应该执行什么
         * 回调函数，是异步编程的一种模式
         */
        //异步+回调函数
        producer.send(record ,
                new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        //若消息发送成功,返回一个null，若失败,exception不是null
                        if(exception == null){
                            System.out.println( metadata.timestamp() + metadata.partition());
                        }

                    }
                }
        );


    }


}
