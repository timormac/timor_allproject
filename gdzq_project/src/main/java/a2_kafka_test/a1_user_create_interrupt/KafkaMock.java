package a2_kafka_test.a1_user_create_interrupt;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
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

        /**码映射
         * 上传身份证 : '12100', '22146', '22107', '22135'
         * 个人信息修改: '22145', '22111', '22224', '22241','22123', '22106'
         * 视频见证:'22109', '22144', '22108', '22182', '22160'
         * 设置密码:'12104', '33500'
         * 风险评测:'22113', '33232', '22110'
         * 问卷回访:'22122', '22128', '22115'
         * 选择市场:'22123', '22106'
         * 三方存管:'22112'
         */

        String topic = "stream_in.rt_crhkh_crh_wskh_userqueryextinfo";
        //String server = "localhost:9092";
        String server = "10.84.187.61:9097,10.84.187.62:9097,10.84.187.63:9097";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,server );
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> producer =  new KafkaProducer<>(properties);


        ArrayList<DataTemplate> list = new ArrayList<>();

        list.add( new DataTemplate("17832300656","111", 2) );

        list.add( new DataTemplate("17832300656","222", 15) );

        list.add( new DataTemplate("17832300656","222", 3) );

        list.add( new DataTemplate("17832300656","222", 16) );


        for (DataTemplate dataTemplate : list) {
            //延迟秒数发送消息
            Thread.sleep(dataTemplate.delaySeconds*1000L);
            dataTemplate.upsertDatetime();
            ProducerRecord record = new ProducerRecord<>(topic,dataTemplate.jsonData);

            System.out.println( "发送一条数据:"+record.value() );

            //同步发送
            producer.send(record).get();
        }








    }
}
