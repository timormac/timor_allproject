package timor.backpress;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import timor.utils.A0_Config;

import java.util.Properties;
import java.util.Random;

/**
 * @Author Timor
 * @Date 2023/12/18 19:44
 * @Version 1.0
 */
public class A1_kafkaMsgMk {


    public static void main(String[] args) throws InterruptedException {

        Properties properties = new Properties();
        //使用这种枚举的方式获取参数
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, A0_Config.KAFKA_SERVERS );
        //指定key的序列化类型
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //指定value的序列化类型
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        String topic = "flink_optimize";

        //默认是异步发送,加上.get()就是同步发送
        int flag = 0;
        Random random = new Random();
        while (true){
            double money = Math.random()*1000;
            String id = "" + random.nextInt(5000) ;
            String s="{\"address\":\"北京市\",\"age\":20,\"name\":\"张三\"" +
                    ",\"id\":"+ "\""+id +"\"" +
                    ",\"money\":"+ money+
                    "}" ;
            kafkaProducer.send( new ProducerRecord<>(topic,s));
            Thread.sleep(1);
            flag++;
            if(flag%10000 ==0) System.out.println(flag);
        }
    }
}
