package timor.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

/**
 * @Author Timor
 * @Date 2023/12/18 19:32
 * @Version 1.0
 */
public class TmKafkaUtils {


    private TmKafkaUtils(){};

    public static KafkaSource<String>  getKafkaSource  (String topic,String groupid){
       //从kafka获取
        return  KafkaSource.<String>builder() //builder前面要指定范型方法
               .setBootstrapServers(A0_Config.KAFKA_SERVERS)
               .setGroupId(groupid)
               .setTopics(topic)
               .setValueOnlyDeserializer(   new SimpleStringSchema()  )//指定从kafka获取的数据的反序列化器
               .setStartingOffsets(OffsetsInitializer.latest())  //设置消费起始offset
               .build();

   }






}
