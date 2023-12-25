package com.lpc.kafka.demo;

import com.lpc.datamock.tools.A0_PropertyUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

/**
 * @Author Timor
 * @Date 2023/10/21 20:46
 * @Version 1.0
 */
public class Producer3 {
    public static void main(String[] args) throws IOException {

        Properties file = new A0_PropertyUtils("kafka_config.properties").getProperty();
        Properties properties = new Properties();

        //使用这种枚举的方式获取参数
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, file.getProperty("bootstrap_servers") );
        //指定key的序列化类型
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //指定value的序列化类型
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);


        String topic = "terminal_produce";

        for (int i = 0; i <5 ; i++) {

            ProducerRecord<String, String> record3 = new ProducerRecord<>(topic,"ccc");
            //多个回调参数
            kafkaProducer.send( record3, new Callback(){
                //回调函数,当收到ack时执行
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null )System.out.println("异步返回信息"+metadata.topic()+metadata.partition());
                    else System.out.println(exception);;
                }
            });
        }

        kafkaProducer.close();



    }
}
