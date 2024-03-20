package a2_kafka_test.a1_user_create_interrupt;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @Title: KafkaConsumer1
 * @Package: PACKAGE_NAME
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/18 09:42
 * @Version:1.0
 */
public class KafkaConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        //String server = "localhost:9092";
        String server = "10.84.187.61:9097,10.84.187.62:9097,10.84.187.63:9097";

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,server);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"gdzq");


        org.apache.kafka.clients.consumer.KafkaConsumer<String,String> kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);

        /**
         * 订阅主题,这种情况一般是自己使用
         */
        ArrayList<String> list = new ArrayList<>();
        list.add("stream_in.rt_crhkh_crh_wskh_userqueryextinfo");
        list.add("stream_crhkh_crh_wskh_mid");
        list.add("stream_mot_stream_account_break_ths");
        list.add("stream_mot_stream_account_break_ths_cc");
        list.add("stream_mot_stream_account_break_ths_cc_mid");
        list.add("stream_mot_stream_account_break_ths_mot_mid");


        //订阅主题,传一个Collctions,若只订阅一个主题,就只放一个元素就行
        kafkaConsumer.subscribe(list);
        while (true){
            //手动设定拉取频率一次拉取的是一个batch的所有数据
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(2*1000L);
            for (ConsumerRecord<String, String> record : poll) {


                if(record.topic().equals("stream_in.rt_crhkh_crh_wskh_userqueryextinfo")){
                    System.out.println("kafka源数据新增一条:"+record.value() );
                }else if (record.topic().equals("stream_crhkh_crh_wskh_mid")){
                    System.out.println("任务1过滤申请中,按天/手机号/flag开窗取1条:"+record.value() );

                }else if (record.topic().equals("stream_mot_stream_account_break_ths")){
                    System.out.println("任务2/任务3输出到同一条流中:"+record.value() );

                }else if (record.topic().equals("stream_mot_stream_account_break_ths_cc")){
                    System.out.println("任务4视频和非视频多条,按mobile/channel_code输出最早1条"+record.value() );

                }else if (record.topic().equals("stream_mot_stream_account_break_ths_cc_mid")){
                    System.out.println("任务6/任务7输出到同一条流中:"+record.value() );

                }else if (record.topic().equals("stream_mot_stream_account_break_ths_mot_mid")){

                    System.out.println("任务8:视频和非视频多条,按mobile/channel_code输出最早1条"+record.value() );
                }else{
                    System.out.println("不该存在的topic");
                }


            }

        }




    }
}
