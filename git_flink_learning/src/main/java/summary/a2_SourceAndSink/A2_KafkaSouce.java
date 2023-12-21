package summary.a2_SourceAndSink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Author Timor
 * @Date 2023/12/21 18:40
 * @Version 1.0
 */
public class A2_KafkaSouce {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO  -------------------------------------fromSouce-------------------------------------------
        //第一种直接设定
        KafkaSourceBuilder<String> builder2 = KafkaSource.<String>builder()
                .setProperties(new Properties());

        //第二种直接配置文件提交
        KafkaSourceBuilder<String> builder = KafkaSource
                .<String>builder()
                .setTopics("aa")
                .setBootstrapServers("")
              //.setStartingOffsets()  //这里有很多规则,最早，最新，当前偏移量等,还有一些
                .setGroupId("")
                .setValueOnlyDeserializer( new SimpleStringSchema() ) //指定从kafka获取的数据的反序列化器
             // .setDeserializer()  这个可能是key的反序列化
                .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 禁用自动重置偏移量

        KafkaSource<String> source = builder.build();
        DataStreamSource<String> fileDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafkasource");

        //TODO  -------------------------------------addSouce-------------------------------------------
        //addsource里面有更细致的管理

        //实现sourceFunction的kafka有3个,FlinkKafkaShuffleConsumer，FlinkKafkaConsumer，FlinkKafkaConsumerBase
        //FlinkKafkaConsumer过时了,FlinkKafkaShuffleConsumer是他的继承类，FlinkKafkaConsumerBase是kafka消费者的基类

        Properties properties = new Properties();
        //将自动提交关闭
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        FlinkKafkaConsumer kafka = new FlinkKafkaConsumer("aa", new SimpleStringSchema(), properties);

        //FlinkKafkaConsumerBase里有一些参数结合kafka和flink的方法

        kafka
                .setCommitOffsetsOnCheckpoints(true)  //设置checkpoint提交,设置这个后kafka的自动提交会失效
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks())  //设置水位线
                 .setStartFromTimestamp(1000L)   //从某一时间开始消费
                .setStartFromEarliest()
                .setStartFromLatest()
                .setStartFromGroupOffsets() ; //从消费者偏移量来消费

        DataStreamSource dataStreamSource = env.addSource(kafka);


    }
}
