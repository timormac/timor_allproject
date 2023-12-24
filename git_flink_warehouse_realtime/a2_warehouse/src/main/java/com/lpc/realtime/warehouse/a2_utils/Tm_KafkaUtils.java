package com.lpc.realtime.warehouse.a2_utils;

import com.google.inject.internal.util.$StackTraceElements;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

/**
 * @Author Timor
 * @Date 2023/12/18 19:32
 * @Version 1.0
 */
public class Tm_KafkaUtils {

    private  static  FlinkKafkaConsumer<String> kafkaConsumer;
    private  static  KafkaSource<String>  kafkaSource;


    public static KafkaSource<String>  getKafkaSource  (String topic,String groupid){

        if (kafkaSource == null){
           kafkaSource = KafkaSource.<String>builder() //builder前面要指定范型方法
                    .setBootstrapServers(A1_ConfigProperty.KAFKA_SERVER)
                    .setGroupId(groupid)
                    .setTopics(topic)
                    .setValueOnlyDeserializer(   new SimpleStringSchema()  )//指定从kafka获取的数据的反序列化器
                    .setStartingOffsets(OffsetsInitializer.latest())  //设置消费起始offset
                    .build();
        }
        return  kafkaSource;
   }


    public static  FlinkKafkaConsumer<String>  getKafkaConsumer(String topic,String groupId){
        return getKafkaConsumer(topic,groupId,false);
    }

    public static  FlinkKafkaConsumer<String>  getKafkaConsumer(String topic,String groupId,boolean checkpointCommit){

        if(kafkaConsumer == null){
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,A1_ConfigProperty.KAFKA_SERVER );

            //如果设置checkpoint提交，则自动提交设为false
            if( checkpointCommit ){
                properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false) );
            }else {
                properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(true) );
            }

            kafkaConsumer = new FlinkKafkaConsumer<String>(
                    topic,
                    new KafkaDeserializationSchema<String>() {
                        @Override
                        public boolean isEndOfStream(String nextElement) {
                            return false;
                        }

                        @Override
                        public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                            if (record == null || record.value() == null) {
                                return "";
                            } else {
                                return new String(record.value());
                            }
                        }

                        @Override
                        public TypeInformation<String> getProducedType() {
                            return BasicTypeInfo.STRING_TYPE_INFO;
                        }
                    },
                    properties);

            //设置通过checkpoint提交
            kafkaConsumer.setCommitOffsetsOnCheckpoints(checkpointCommit);
        }
        return  kafkaConsumer;
   }







}
