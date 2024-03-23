package a2_kafka_test.test2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class 测试合并过滤_数据生成 {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        String topic = "stream_mot_stream_account_break_ths_cc_mid";
        //String server = "10.84.187.61:9097,10.84.187.62:9097,10.84.187.63:9097";

        String server =  "localhost:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,server );
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        KafkaProducer<String,String> producer =  new KafkaProducer<>(properties);
        ArrayList<CcMidTemplate> list = new ArrayList<>();



        //上传身份证12100
        list.add( new CcMidTemplate("20240322","112778845566") );
        list.add( new CcMidTemplate("20240322","112778845566") );
        list.add( new CcMidTemplate("20240322","112778845566") );


        for (CcMidTemplate data : list) {
            //延迟秒数发送消息
            Thread.sleep(2000L);
            System.out.println("休息2s");

            data.upsertAccourTime();

            ProducerRecord record = new ProducerRecord<>(topic,data.jsonData);
            System.out.println( "发送一条数据:"+record.value() );
            //同步发送
            producer.send(record).get();
        }

    }
}
