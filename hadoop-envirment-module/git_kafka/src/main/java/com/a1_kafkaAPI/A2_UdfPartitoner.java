package com.a1_kafkaAPI;

import com.utils.A1_Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

/**
 * @Title: A2_UdfPartitoner
 * @Package: com.a1_kafkaAPI
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/25 17:11
 * @Version:1.0
 */
public class A2_UdfPartitoner {

    public static void main(String[] args) {

        Properties properties = new Properties();
        //指定分区器的类
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG,udfPatitioner.class.getName());
       new KafkaProducer<>(properties);
    }

    class udfPatitioner implements Partitioner{
        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            if(valueBytes.equals("aa")) { return  1; }
            return 0;
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {

        }
    }

}
