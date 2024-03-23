package a2_kafka_test.test1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaMockDemo2 {
    public static void main(String[] args) throws InterruptedException, ExecutionException {

        /**码映射
         * 上传身份证 : '12100', '22146', '22107', '22135'
         * 个人信息修改: '22145', '22111', '22224', '22241','22123', '22106'
         * 视频见证:'22109', '22144', '22108', '22182', '22160'
         * 设置密码:'12104', '33500'
         * 风险评测:'22113', '33232', '22110'
         * 问卷回访:'22122', '22128', '22115'
         * 选择市场:'22123', '22106'
         * 三方存管:'22112'
         * 开户提交申请:22114
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

        list.add( new DataTemplate("111111111111","12100", 0) );
        list.add( new DataTemplate("111111111111","22135", 1) );
        list.add( new DataTemplate("111111111111","22145", 1) );
        //正常结束
        list.add( new DataTemplate("111111111111","22114", 1) );




        for (DataTemplate dataTemplate : list) {
            //延迟秒数发送消息
            Thread.sleep(dataTemplate.delaySeconds*1000L);
            System.out.println("休息"+dataTemplate.delaySeconds);
            dataTemplate.upsertDatetime();


            ProducerRecord record = new ProducerRecord<>(topic,dataTemplate.jsonData);
            System.out.println(dataTemplate.jsonData);

            System.out.println( "发送一条数据:"+record.value() );

            //同步发送
            producer.send(record).get();
        }




    }
}
