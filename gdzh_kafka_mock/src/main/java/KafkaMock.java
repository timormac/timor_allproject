import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @Title: KafkaMock
 * @Package: PACKAGE_NAME
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/17 18:42
 * @Version:1.0
 */
public class KafkaMock {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        String topic = "rt_crhkh_crh_wskh_userqueryextinfo";
        String server = "localhost:9092";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,server );
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaProducer<String,String> producer =  new KafkaProducer<>(properties);



        ProducerRecord record = new ProducerRecord<>(topic,"a");


        //同步发送
        producer.send(record).get();

    }
}
