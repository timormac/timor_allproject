package com.a1_kafkaAPI;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @Title: A9_Optmize
 * @Package: com.a1_kafkaAPI
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/25 17:26
 * @Version:1.0
 */
public class A9_Optmize {

    public static void main(String[] args) {

        Properties properties = new Properties();
        //batch大小
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"");
        //linger.ms
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"");
        //是否压缩
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"");
        //缓冲区recordAccumulator大小
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG,"");
        //设置重试，默认时int最大值,会一直卡住。 3次
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,"10");
        //设置ack
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"-1");

    }
}
