package a2_kafka_test.test2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import a2_kafka_test.test1.DataTemplate;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class 单点测试30分钟视频 {
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

        //会话窗口10s和20s  任务2视频30min 改为20s  任务3非视频8分钟 改为10s

        //上传身份证12100
        list.add( new DataTemplate("777777777777777","12100", 0) );
        //上传身份证12100,相同flag应被过滤掉
        list.add( new DataTemplate("777777777777777","12100", 2) );
        //1S后到22109上传视频，flag不同，应新增一条
        list.add( new DataTemplate("777777777777777","22109", 1) );
        //视频见证 12s消息到来之前,2个会话窗口都触发,3会过滤掉视频,只有任务2输出一条
        list.add( new DataTemplate("777777777777777","22144", 40) );
        //视频见证20s消息到来之前,2个会话窗口都触发,3会过滤掉视频,任务2会话窗口输出一条，但是rk =1过滤掉了
        list.add( new DataTemplate("777777777777777","22108", 40) );

        //结束无消息,窗口返回最后条数据,满足过滤条件, 但是rk过滤掉



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


        /*

kafka源数据新增一条:{"message":{"data":{"ID_NO":"130421200508230659","REQUEST_NO":"2593832","BUSINESS_FLAG_LAST":"12100","CLIENT_NAME":"刘嘉浩","BRANCH_NO":" ","USER_ID":"3413602","MOBILE_TEL":"777777777777777","REQUEST_STATUS":"0","CHANNEL_CODE":"10632","LAST_UPDATE_DATETIME":"2024-03-21 16:15:55","BIRTHDAY":"20050823"}}}
任务1过滤申请中,按天/手机号/flag开窗取1条:{"mobile_tel":"777777777777777","branch_no":" ","business_flag_last":"12100","channel_code":"10632","last_update_detetime":"2024-03-21 16:15:55","client_name":"刘嘉浩","user_id":"3413602","id_no":"130421200508230659","birthday":"20050823","request_no":"2593832"}
kafka源数据新增一条:{"message":{"data":{"ID_NO":"130421200508230659","REQUEST_NO":"2593832","BUSINESS_FLAG_LAST":"12100","CLIENT_NAME":"刘嘉浩","BRANCH_NO":" ","USER_ID":"3413602","MOBILE_TEL":"777777777777777","REQUEST_STATUS":"0","CHANNEL_CODE":"10632","LAST_UPDATE_DATETIME":"2024-03-21 16:15:58","BIRTHDAY":"20050823"}}}
kafka源数据新增一条:{"message":{"data":{"ID_NO":"130421200508230659","REQUEST_NO":"2593832","BUSINESS_FLAG_LAST":"22109","CLIENT_NAME":"刘嘉浩","BRANCH_NO":" ","USER_ID":"3413602","MOBILE_TEL":"777777777777777","REQUEST_STATUS":"0","CHANNEL_CODE":"10632","LAST_UPDATE_DATETIME":"2024-03-21 16:15:59","BIRTHDAY":"20050823"}}}
任务1过滤申请中,按天/手机号/flag开窗取1条:{"mobile_tel":"777777777777777","branch_no":" ","business_flag_last":"22109","channel_code":"10632","last_update_detetime":"2024-03-21 16:15:59","client_name":"刘嘉浩","user_id":"3413602","id_no":"130421200508230659","birthday":"20050823","request_no":"2593832"}
任务2/任务3输出到同一条流中:{"event_id":"019-1aaaa","event_name":"开户流程中断30min转化-cc（视频见证）","client_name":"刘嘉浩","channel_name":"APP应用市场","branch_name":"","last_mobilenum":"77777777","request_no":"2593832","business_name":"协议签署","step_code":"4","step_name":"视频见证","channel_type":"znwh","mobile":"777777777777777","branch_code":" ","occur_date":"20240321","occur_time":"16:16:29","birthday":"20050823","last_update_detetime":"2024-03-21 16:15:59"}
任务4视频和非视频多条,按mobile/channel_code输出最早1条{"event_id":"019-1aaaa","event_name":"开户流程中断30min转化-cc（视频见证）","client_name":"刘嘉浩","channel_name":"APP应用市场","branch_name":"","last_mobilenum":"77777777","request_no":"2593832","business_name":"协议签署","step_code":"4","step_name":"视频见证","channel_type":"znwh","mobile":"777777777777777","branch_code":" ","occur_date":"20240321","occur_time":"16:16:29","birthday":"20050823"}
kafka源数据新增一条:{"message":{"data":{"ID_NO":"130421200508230659","REQUEST_NO":"2593832","BUSINESS_FLAG_LAST":"22144","CLIENT_NAME":"刘嘉浩","BRANCH_NO":" ","USER_ID":"3413602","MOBILE_TEL":"777777777777777","REQUEST_STATUS":"0","CHANNEL_CODE":"10632","LAST_UPDATE_DATETIME":"2024-03-21 16:16:39","BIRTHDAY":"20050823"}}}
任务1过滤申请中,按天/手机号/flag开窗取1条:{"mobile_tel":"777777777777777","branch_no":" ","business_flag_last":"22144","channel_code":"10632","last_update_detetime":"2024-03-21 16:16:39","client_name":"刘嘉浩","user_id":"3413602","id_no":"130421200508230659","birthday":"20050823","request_no":"2593832"}
kafka源数据新增一条:{"message":{"data":{"ID_NO":"130421200508230659","REQUEST_NO":"2593832","BUSINESS_FLAG_LAST":"22108","CLIENT_NAME":"刘嘉浩","BRANCH_NO":" ","USER_ID":"3413602","MOBILE_TEL":"777777777777777","REQUEST_STATUS":"0","CHANNEL_CODE":"10632","LAST_UPDATE_DATETIME":"2024-03-21 16:17:19","BIRTHDAY":"20050823"}}}
任务1过滤申请中,按天/手机号/flag开窗取1条:{"mobile_tel":"777777777777777","branch_no":" ","business_flag_last":"22108","channel_code":"10632","last_update_detetime":"2024-03-21 16:17:19","client_name":"刘嘉浩","user_id":"3413602","id_no":"130421200508230659","birthday":"20050823","request_no":"2593832"}

         */



    }
}
