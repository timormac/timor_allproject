package a2_kafka_test.test2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import a2_kafka_test.test1.DataTemplate;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class 单点测试8分钟非视频 {
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

        //会话窗口10s和20s  任务2视频30min 改为20s  任务3非视频8分钟 改为10s

        //上传身份证12100
        list.add( new DataTemplate("5555555555555","12100", 0) );
        //上传身份证12100,相同flag应被过滤掉
        list.add( new DataTemplate("5555555555555","12100", 2) );
        //1S后到上传身份证第二步22135，flag不同，应新增一条
        list.add( new DataTemplate("5555555555555","22135", 1) );
        //信息修改30s消息到来之前,2个会话窗口都触发,到个人身份修改第一步22145,但是任务2会过滤掉非视频,只有任务3输出一条
        list.add( new DataTemplate("5555555555555","22145", 30) );
        //信息修改30s消息到来之前,2个会话窗口都触发,但是任务2会过滤掉非视频,任务3会话窗口输出一条，但是rk =1过滤掉了，不输出,30s 到个人身份修改第二步22224,
        list.add( new DataTemplate("5555555555555","22111", 30) );
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
        kafka源数据新增一条:{"message":{"data":{"ID_NO":"130421200508230659","REQUEST_NO":"2593832","BUSINESS_FLAG_LAST":"12100","CLIENT_NAME":"刘嘉浩","BRANCH_NO":" ","USER_ID":"3413602","MOBILE_TEL":"5555555555555","REQUEST_STATUS":"0","CHANNEL_CODE":"10632","LAST_UPDATE_DATETIME":"2024-03-21 15:29:02","BIRTHDAY":"20050823"}}}
        任务1过滤申请中,按天/手机号/flag开窗取1条:{"mobile_tel":"5555555555555","branch_no":" ","business_flag_last":"12100","channel_code":"10632","last_update_detetime":"2024-03-21 15:29:02","client_name":"刘嘉浩","user_id":"3413602","id_no":"130421200508230659","birthday":"20050823","request_no":"2593832"}
        kafka源数据新增一条:{"message":{"data":{"ID_NO":"130421200508230659","REQUEST_NO":"2593832","BUSINESS_FLAG_LAST":"12100","CLIENT_NAME":"刘嘉浩","BRANCH_NO":" ","USER_ID":"3413602","MOBILE_TEL":"5555555555555","REQUEST_STATUS":"0","CHANNEL_CODE":"10632","LAST_UPDATE_DATETIME":"2024-03-21 15:29:05","BIRTHDAY":"20050823"}}}
        kafka源数据新增一条:{"message":{"data":{"ID_NO":"130421200508230659","REQUEST_NO":"2593832","BUSINESS_FLAG_LAST":"22135","CLIENT_NAME":"刘嘉浩","BRANCH_NO":" ","USER_ID":"3413602","MOBILE_TEL":"5555555555555","REQUEST_STATUS":"0","CHANNEL_CODE":"10632","LAST_UPDATE_DATETIME":"2024-03-21 15:29:06","BIRTHDAY":"20050823"}}}
        任务1过滤申请中,按天/手机号/flag开窗取1条:{"mobile_tel":"5555555555555","branch_no":" ","business_flag_last":"22135","channel_code":"10632","last_update_detetime":"2024-03-21 15:29:06","client_name":"刘嘉浩","user_id":"3413602","id_no":"130421200508230659","birthday":"20050823","request_no":"2593832"}
        任务2/任务3输出到同一条流中:{"event_id":"019-1bbbb","event_name":"开户流程中断8min转化-cc（非视频见证）","client_name":"刘嘉浩","channel_name":"APP应用市场","branch_name":"","last_mobilenum":"555555","request_no":"2593832","business_name":"证件信息识别","step_code":"1","step_name":"上传身份证","channel_type":"znwh","mobile":"5555555555555","branch_code":" ","occur_date":"20240321","occur_time":"15:29:28","birthday":"20050823","last_update_detetime":"2024-03-21 15:29:06"}
        任务4视频和非视频多条,按mobile/channel_code输出最早1条{"event_id":"019-1bbbb","event_name":"开户流程中断8min转化-cc（非视频见证）","client_name":"刘嘉浩","channel_name":"APP应用市场","branch_name":"","last_mobilenum":"555555","request_no":"2593832","business_name":"证件信息识别","step_code":"1","step_name":"上传身份证","channel_type":"znwh","mobile":"5555555555555","branch_code":" ","occur_date":"20240321","occur_time":"15:29:28","birthday":"20050823"}
        kafka源数据新增一条:{"message":{"data":{"ID_NO":"130421200508230659","REQUEST_NO":"2593832","BUSINESS_FLAG_LAST":"22145","CLIENT_NAME":"刘嘉浩","BRANCH_NO":" ","USER_ID":"3413602","MOBILE_TEL":"5555555555555","REQUEST_STATUS":"0","CHANNEL_CODE":"10632","LAST_UPDATE_DATETIME":"2024-03-21 15:29:36","BIRTHDAY":"20050823"}}}
        任务1过滤申请中,按天/手机号/flag开窗取1条:{"mobile_tel":"5555555555555","branch_no":" ","business_flag_last":"22145","channel_code":"10632","last_update_detetime":"2024-03-21 15:29:36","client_name":"刘嘉浩","user_id":"3413602","id_no":"130421200508230659","birthday":"20050823","request_no":"2593832"}
        kafka源数据新增一条:{"message":{"data":{"ID_NO":"130421200508230659","REQUEST_NO":"2593832","BUSINESS_FLAG_LAST":"22111","CLIENT_NAME":"刘嘉浩","BRANCH_NO":" ","USER_ID":"3413602","MOBILE_TEL":"5555555555555","REQUEST_STATUS":"0","CHANNEL_CODE":"10632","LAST_UPDATE_DATETIME":"2024-03-21 15:30:06","BIRTHDAY":"20050823"}}}
        任务1过滤申请中,按天/手机号/flag开窗取1条:{"mobile_tel":"5555555555555","branch_no":" ","business_flag_last":"22111","channel_code":"10632","last_update_detetime":"2024-03-21 15:30:06","client_name":"刘嘉浩","user_id":"3413602","id_no":"130421200508230659","birthday":"20050823","request_no":"2593832"}

        */


    }
}
