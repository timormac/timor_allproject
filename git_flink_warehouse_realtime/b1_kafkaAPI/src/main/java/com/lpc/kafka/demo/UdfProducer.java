package com.lpc.kafka.demo;

import com.lpc.datamock.tools.A0_PropertyUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @Title: UdfProducer
 * @Package: com.lpc.kafka.demo
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/21 15:55
 * @Version:1.0
 */
public class UdfProducer {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        Properties file = new A0_PropertyUtils("kafka_config.properties").getProperty();
        Properties properties = new Properties();

        //使用这种枚举的方式获取参数
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, file.getProperty("bootstrap_servers") );
        //指定key的序列化类型
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //指定key的序列化类型
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);


        String topic = "terminal_produce";

        //默认是异步发送,加上.get()就是同步发送
        kafkaProducer.send( new ProducerRecord<>(topic,"aaa")).get();

        long stmap = System.currentTimeMillis()-10;
        //指定topic,分区,时间戳(null默认系统时间,key,value,header不知道干什么用的
        ProducerRecord<String, String> record2 = new ProducerRecord<String, String>(
           topic,1, stmap,"key","value",null);

        kafkaProducer.send(record2);

        ProducerRecord<String, String> record3 = new ProducerRecord<>(topic,"bbb");
        //多个回调参数
        kafkaProducer.send( record3, new Callback(){
            //回调函数,当收到ack时执行
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if(exception == null )System.out.println("异步返回信息"+metadata.topic()+metadata.partition());
                else System.out.println(exception);;
            }
        });
        kafkaProducer.close();

    }
}
