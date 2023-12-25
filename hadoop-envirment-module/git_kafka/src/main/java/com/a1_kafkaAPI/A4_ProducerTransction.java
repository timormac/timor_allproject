package com.a1_kafkaAPI;

import com.utils.A2_KafkaUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Title: A4_ProducerTransction
 * @Package: com.a1_kafkaAPI
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/25 19:19
 * @Version:1.0
 */
public class A4_ProducerTransction {
    public static void main(String[] args) {
        Properties properties = new Properties();

        //必须手动设置全局事务id
        properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"");

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String,String> record = new ProducerRecord<>("topic","a");


        //初始化事务
        producer.initTransactions();
        //开始事务
        producer.beginTransaction();
        try{
            producer.send(record);
            //提交事务
            producer.commitTransaction();
        }catch (Exception e){
            //遇到异常回滚事务
            producer.abortTransaction();
        }


    }
}
