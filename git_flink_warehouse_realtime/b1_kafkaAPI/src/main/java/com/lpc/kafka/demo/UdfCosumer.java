package com.lpc.kafka.demo;

import com.lpc.datamock.tools.A0_PropertyUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @Title: UdfCosumer
 * @Package: com.lpc.kafka.demo
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/21 18:35
 * @Version:1.0
 */
public class UdfCosumer {

    public static void main(String[] args) throws IOException {
        Properties file = new A0_PropertyUtils("kafka_config.properties").getProperty();
        Properties properties = new Properties();

        //使用这种枚举的方式获取参数
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, file.getProperty("bootstrap_servers") );
        //指定key的反序列化类型
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //指定key的反序列化类型
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //设置消费者组的名称
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,file.getProperty("groupId"));

        //关闭自动提交offset
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");


        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

//        指定消费单独分区，如果只有一个消费者那么别的分区不会消费，后加入消费者才会去消费
//        ArrayList<TopicPartition> arr = new ArrayList<>();
//        arr.add( new TopicPartition("topic",1) );
//        consumer.assign( arr );


        ArrayList<String> topicArr = new ArrayList<>();
        topicArr.add("terminal_produce");

        //可以消费多个主题
        consumer.subscribe(topicArr);
        while(true){
            //1s取拉取一次数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1L));

            //遍历完事后，应该就清空数据了。可能是返回了一个新的arr对象
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record );
            }

            //手动提交offset
            consumer.commitAsync();
        }


    }


}
