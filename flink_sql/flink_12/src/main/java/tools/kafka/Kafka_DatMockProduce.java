package tools.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import tools.kafka.dao.AbstractDelayData;
import tools.kafka.dao.MockData;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @Title: KafkaDatMockProduce
 * @Package: tools.kafka
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/23 10:14
 * @Version:1.0
 */
public  class Kafka_DatMockProduce {

    public static KafkaSender getKafkaProducer( String servers,String topic ){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,servers );
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaSender(properties,topic);
    }

    public static class KafkaSender<E>{
        Properties properties;
        String topic;
        KafkaProducer<String,String> producer;

        public KafkaSender(Properties properties, String topic) {
            this.properties = properties;
            this.topic = topic;
            getProducer();
        }

        private void  getProducer(){
            this.producer = new KafkaProducer<>(properties);
        }

        public void sendData(List<MockData> list) throws ExecutionException, InterruptedException {
            for (MockData elem : list) {
            ProducerRecord record = new ProducerRecord<>(topic,elem.getValue());
            System.out.println( "发送一条数据:"+record.value() );
            producer.send(record).get();
            }
        }

        public void sendDelayData(List<AbstractDelayData> list) throws ExecutionException, InterruptedException {
            for (AbstractDelayData elem : list) {
                //先休息
                Thread.sleep( elem.getDelaySeconds()*1000L );
                //更新data
                elem.excuteUpdate();
                ProducerRecord record = new ProducerRecord<>(topic,elem.getValue());
                System.out.println( "发送一条数据:"+record.value() );
                producer.send(record).get();
            }
        }

    }


}
