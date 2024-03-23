package a2_kafka_test.test1;

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
public class KafkaMockDemo1 {
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

        //会话窗口8s和20s

        //上传身份证12100
        list.add( new DataTemplate("5555555555555","12100", 0) );
        //上传身份证12100,相同flag应被过滤掉
        list.add( new DataTemplate("444444","12100", 2) );
        //1S后到上传身份证第二步22135
        list.add( new DataTemplate("444444","22135", 1) );
        //9s后到个人身份修改22145,会话超时第一次触发,应该返回一条消息为22135,
        list.add( new DataTemplate("444444","22145", 11) );
        //11s 到个人身份修改第二步22224,会话超时第二次，但是前面发过消息不应该有数据，（重复测试）
        list.add( new DataTemplate("444444","22111", 11) );
        //25s后到视频22109超时,之前都在20内，视频会话窗口在这里第一次触发，在非视频里触发但不应该有数据
        list.add( new DataTemplate("444444","22109", 25) );
        //25s后到视频22144,视频会话第二次触发,但不应该再有数据
        list.add( new DataTemplate("444444","22144", 25) );
        //1s后开户提交申请
        list.add( new DataTemplate("444444","22114", 1) );


        for (DataTemplate dataTemplate : list) {
            //延迟秒数发送消息
            Thread.sleep(dataTemplate.delaySeconds*1000L);
            System.out.println("休息"+dataTemplate.delaySeconds);
            dataTemplate.upsertDatetime();
            ProducerRecord record = new ProducerRecord<>(topic,dataTemplate.jsonData);

            System.out.println( "发送一条数据:"+record.value() );

            //同步发送
            producer.send(record).get();
        }








    }
}
